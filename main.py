from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import os
import concurrent.futures
import time
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
import duckdb
from typing import Optional, Dict, List

# ==========================================
# 0. Database 초기화
# ==========================================
DB_PATH = "financial_data.duckdb"

def init_db():
    conn = duckdb.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cached_financials (
            corp_code VARCHAR,
            year INTEGER,
            quarter INTEGER,
            report_code VARCHAR,
            fs_div VARCHAR,
            account_id VARCHAR,
            account_nm VARCHAR,
            thstrm_amount BIGINT,
            PRIMARY KEY (corp_code, year, report_code, fs_div, account_id)
        )
    """)
    conn.close()

# 앱 시작 시 DB 초기화
init_db()

# ==========================================
# 1. DART 고유번호(Corp Code) 관리 함수
# ==========================================

def get_company_codes(api_key: str, cache_file: str = "company_codes_cache.json") -> Optional[Dict[str, str]]:
    """
    Open DART에서 고유번호(8자리)를 받아와 캐싱하고, 회사명:고유번호 딕셔너리를 반환합니다.
    """
    if os.path.exists(cache_file):
        try:
            cache_df = pd.read_json(cache_file)
            if not cache_df.empty:
                cache_df['corp_code'] = cache_df['corp_code'].astype(str).str.zfill(8)
                print(f"📁 캐시 파일 로드 완료: {len(cache_df)}개 기업")
                return cache_df.set_index('corp_name')['corp_code'].to_dict()
        except Exception as e:
            print(f"⚠️ 캐시 파일 손상 (재다운로드 진행): {e}")

    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {'crtfc_key': api_key}

    try:
        print("⬇️ DART에서 최신 기업 고유번호를 다운로드 중...")
        response = requests.get(url, params=params)

        if response.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                xml_filename = zip_file.namelist()[0]
                with zip_file.open(xml_filename) as f:
                    tree = ET.parse(f)
                    root = tree.getroot()

                    data_list = []
                    for corp in root.findall('.//list'):
                        code = corp.findtext('corp_code', '').strip()
                        name = corp.findtext('corp_name', '').strip()
                        if code and name:
                            data_list.append({'corp_name': name, 'corp_code': code})

            if data_list:
                df = pd.DataFrame(data_list)
                df['corp_code'] = df['corp_code'].astype(str)
                df.to_json(cache_file, orient='records', force_ascii=False)
                print(f"✅ 고유번호 다운로드 및 캐싱 완료 ({len(df)}개)")
                return df.set_index('corp_name')['corp_code'].to_dict()
        
        print("❌ 고유번호 다운로드 실패 (API 응답 오류)")
        return None

    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        return None

def search_company_code(api_key: str, company_name: str) -> Optional[str]:
    """
    회사명으로 고유번호를 검색합니다 (정확 일치 -> 부분 일치 순).
    """
    codes = get_company_codes(api_key)
    if not codes:
        return None

    if company_name in codes:
        code = codes[company_name]
        print(f"🔍 '{company_name}' 검색 성공 (정확 일치) -> Code: {code}")
        return str(code).zfill(8)

    candidates = [name for name in codes.keys() if company_name in name]
    if len(candidates) == 1:
        matched_name = candidates[0]
        code = codes[matched_name]
        print(f"🔍 '{company_name}' 검색 성공 ('{matched_name}' 부분 일치) -> Code: {code}")
        return str(code).zfill(8)
    elif len(candidates) > 1:
        print(f"⚠️ '{company_name}' 검색 결과가 너무 많습니다: {candidates[:5]} ...")
        return None
    else:
        print(f"❌ '{company_name}' 회사를 찾을 수 없습니다.")
        return None

# ==========================================
# 2. 재무제표 데이터 수집 함수
# ==========================================

def get_financial_data(api_key: str, corp_code: str, year: int, report_type: str, fs_div: str, session: requests.Session = None) -> Optional[pd.DataFrame]:
    """
    특정 조건(년도, 보고서타입, 구분)의 재무제표 데이터를 가져옵니다.
    """
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {
        'crtfc_key': api_key,
        'corp_code': str(corp_code).zfill(8),
        'bsns_year': str(year),
        'reprt_code': report_type,
        'fs_div': fs_div
    }
    
    try:
        if session:
            res = session.get(url, params=params, timeout=10)
        else:
            res = requests.get(url, params=params, timeout=10)
        data = res.json()
        
        if data['status'] == '000' and data.get('list'):
            df = pd.DataFrame(data['list'])
            numeric_cols = ['thstrm_amount', 'frmtrm_amount', 'bfefrmtrm_amount']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].str.replace(',', ''), errors='coerce')
            return df
        else:
            return None
    except Exception as e:
        print(f"❌ API 호출 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return None

def get_financial_data_from_db(corp_code: str, year: int, report_code: str, fs_div: str) -> Optional[pd.DataFrame]:
    """DB에서 재무 데이터를 조회합니다."""
    try:
        conn = duckdb.connect(DB_PATH)
        query = """
            SELECT account_id, account_nm, thstrm_amount 
            FROM cached_financials 
            WHERE corp_code = ? AND year = ? AND report_code = ? AND fs_div = ?
        """
        df = conn.execute(query, [str(corp_code), int(year), str(report_code), str(fs_div)]).df()
        conn.close()
        
        if not df.empty:
            return df
        return None
    except Exception as e:
        print(f"⚠️ DB 조회 중 오류: {e}")
        return None

def save_financial_data_to_db(df: pd.DataFrame, corp_code: str, year: int, quarter: int, report_code: str, fs_div: str):
    """API에서 가져온 데이터를 DB에 저장(Upsert)합니다."""
    if df is None or df.empty:
        return

    try:
        conn = duckdb.connect(DB_PATH)
        
        # 저장할 주요 항목만 필터링 (매출액, 영업이익)
        key_items = ['ifrs-full_Revenue', 'dart_OperatingIncomeLoss']
        target_df = df[df['account_id'].isin(key_items)].copy()
        
        if target_df.empty:
            conn.close()
            return
            
        # 데이터 준비
        data_to_insert = []
        for _, row in target_df.iterrows():
            data_to_insert.append((
                str(corp_code),
                int(year),
                int(quarter),
                str(report_code),
                str(fs_div),
                row['account_id'],
                row['account_nm'],
                int(row['thstrm_amount']) if pd.notna(row['thstrm_amount']) else 0
            ))
            
        # Upsert 실행
        conn.executemany("""
            INSERT OR REPLACE INTO cached_financials 
            (corp_code, year, quarter, report_code, fs_div, account_id, account_nm, thstrm_amount)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, data_to_insert)
        
        conn.close()
        # print(f"  💾 DB 저장 완료 ({year}년 {quarter}분기)")
        
    except Exception as e:
        print(f"⚠️ DB 저장 실패: {e}")

def get_quarter_info(year_month: int) -> tuple:
    """
    YYYYMM 형식의 입력을 받아 해당 분기 정보를 반환합니다.
    분기말(3,6,9,12)이 아니면 가장 최근 분기말 기준으로 조정합니다.
    """
    year = year_month // 100
    month = year_month % 100

    # 분기 결정
    if month <= 3:
        quarter = 1
        quarter_end_month = 3
        quarter_end_year = year
    elif month <= 6:
        quarter = 2
        quarter_end_month = 6
        quarter_end_year = year
    elif month <= 9:
        quarter = 3
        quarter_end_month = 9
        quarter_end_year = year
    else:
        quarter = 4
        quarter_end_month = 12
        quarter_end_year = year

    return quarter, quarter_end_year, quarter_end_month

def adjust_q4_values(df: pd.DataFrame, year_month: int = None) -> pd.DataFrame:
    """
    DART API에서 가져온 4분기 누적값을 실제 4분기 값으로 조정합니다.
    4분기를 포함하고 있는 모든 해에 대해 Q4 값을 조정합니다.
    """
    if df.empty or '분기' not in df.columns:
        return df

    # 4분기 데이터만 필터링
    q4_data = df[df['분기'] == 4].copy()

    if q4_data.empty:
        return df

    # 모든 해에 대해 Q4 값 조정 적용
    for year in q4_data['년도'].unique():
        # 해당 해의 Q1+Q2+Q3 데이터 합계 계산
        q1_q3_data = df[(df['년도'] == year) & df['분기'].isin([1, 2, 3])]

        if q1_q3_data.empty:
            continue

        # 항목별로 Q1+Q2+Q3 합계 계산 (구분 컬럼 포함)
        q1_q2_q3_sum = {}
        for item in q1_q3_data['항목'].unique():
            for fs_div in q1_q3_data['구분'].unique():
                item_sum = q1_q3_data[(q1_q3_data['항목'] == item) & (q1_q3_data['구분'] == fs_div)]['thstrm_amount'].sum()
                q1_q2_q3_sum[(year, item, fs_div)] = item_sum

        # 해당 해의 Q4 값 조정
        year_q4_data = df[(df['년도'] == year) & (df['분기'] == 4)]
        for idx, row in year_q4_data.iterrows():
            item = row['항목']
            fs_div = row['구분']

            if (year, item, fs_div) in q1_q2_q3_sum:
                adjusted_value = row['thstrm_amount'] - q1_q2_q3_sum[(year, item, fs_div)]
                df.at[idx, 'thstrm_amount'] = adjusted_value

    return df

def collect_quarterly_financials(api_key: str, corp_code: str, year: int, year_month: int = None) -> pd.DataFrame:
    """
    특정 년도의 모든 분기(사업보고서, 1분기, 반기, 3분기) 재무제표를 수집하여 정리합니다.
    year_month가 제공되면 해당 분기부터 직전 4분기 데이터를 수집합니다.
    """
    corp_code = str(corp_code).zfill(8)

    report_types = [
        ('사업보고서', '11011'),
        ('1분기보고서', '11013'),
        ('반기보고서', '11012'),
        ('3분기보고서', '11014')
    ]

    fs_divs = [('연결', 'CFS'), ('별도', 'OFS')]

    all_data = []

    if year_month is not None:
        # YYYYMM 형식 처리
        quarter, quarter_end_year, quarter_end_month = get_quarter_info(year_month)

        # 입력한 해(YYYY 또는 YYYYMM 의 YYYY)기준으로 [YYYY-4] 년 1분기부터 불러오기
        start_year = quarter_end_year - 4
        start_quarter = 1
        end_year = quarter_end_year
        end_quarter = quarter
        if quarter_end_month == 12:
            end_quarter = 4

        # 모든 분기 목록 생성
        quarters_to_collect = []
        current_year = start_year
        current_quarter = start_quarter

        while True:
            quarters_to_collect.append((current_year, current_quarter))

            if current_year == end_year and current_quarter == end_quarter:
                break

            current_quarter += 1
            if current_quarter > 4:
                current_quarter = 1
                current_year += 1

        print(f"\n🔄 [{year_month if year_month else year} 기준/년] {corp_code} 재무데이터 수집 시작 (병렬 처리)...")
        
        
        
        # requests.Session()을 사용하여 연결 재사용
        with requests.Session() as session:
            
            # [최적화 1단계] DB에서 데이터 조회 시도
            # 캐싱된 데이터가 있는지 먼저 확인하고, 없으면 API 호출 대상 리스트(missing_tasks)를 만듭니다.
            missing_tasks = []
            
            # 탐색할 분기 리스트 (최신 -> 과거 순으로 정렬되어 있지는 않음, 필요시 정렬)
            # 여기서는 편의상 quarters_to_collect 순서대로 확인
            
            # [최적화 2단계] 사용할 재무제표 종류(연결/별도) 결정 (API 호출이 필요한 경우에만)
            # 만약 DB에 데이터가 하나도 없다면, Probing을 통해 연결/별도를 결정해야 함.
            determined_fs_divs = fs_divs 
            
            # 1. DB 조회 및 데이터 수집
            for target_year, target_quarter in quarters_to_collect:
                 if target_quarter == 1: report_code = '11013'; report_name = '1분기보고서'
                 elif target_quarter == 2: report_code = '11012'; report_name = '반기보고서'
                 elif target_quarter == 3: report_code = '11014'; report_name = '3분기보고서'
                 else: report_code = '11011'; report_name = '사업보고서'

                 # 연결/별도/둘다 시도 (determined_fs_divs 기준이 아니라, 일단 캐시된게 있는지 확인)
                 # 하지만 캐시된 데이터가 "어떤 fs_div"인지 알아야 하므로, 
                 # 전략:
                 # - 일단 확정된 determined_fs_divs 가 있다면 그것만 조회.
                 # - 아직 확정되지 않았다면(초기 상태), 연결->별도 순으로 DB 조회 시도.
                 
                 found_in_db = False
                 
                 # 만약 fs_div가 확정되지 않았다면, 연결->별도 순으로 DB를 찔러봄
                 current_check_divs = determined_fs_divs
                 
                 for fs_name, fs_code in current_check_divs:
                     db_df = get_financial_data_from_db(corp_code, target_year, report_code, fs_code)
                     if db_df is not None:
                         db_df['보고서명'] = report_name
                         db_df['구분'] = fs_name
                         db_df['년도'] = target_year
                         db_df['분기'] = target_quarter
                         all_data.append(db_df)
                         print(f"  ✅ {target_year}년 {target_quarter}분기 ({fs_name}) - DB(Cache)에서 로드됨")
                         
                         found_in_db = True
                         # 캐시에서 '연결'을 찾았다면, 앞으로는 '연결'만 찾으면 됨
                         if fs_code == 'CFS' and len(determined_fs_divs) > 1:
                             determined_fs_divs = [('연결', 'CFS')]
                         # 캐시에서 '별도'를 찾았고 연결이 없었다면? (이건 확신할 수 없음. 연결 데이터가 누락된 걸수도)
                         # 하지만 통상적으로 최근 데이터가 별도라면 별도 기업일 확률 높음.
                         # 보수적으로: 캐시된게 있으면 그걸 씀.
                         break
                 
                 if not found_in_db:
                     # DB에 없으면 API 호출 목록에 추가
                     missing_tasks.append((target_year, target_quarter, report_code, report_name))

            # 2. API 호출 (DB에 없는 데이터만)
            if missing_tasks:
                print(f"  ⬇️ {len(missing_tasks)}건의 데이터가 DB에 없어 API에서 조회합니다...")
                
                # [Probing] 만약 아직 fs_div가 확정되지 않았다면(여전히 2개라면),
                # missing_tasks 중 가장 최신 분기를 골라 Probing을 한다.
                if len(determined_fs_divs) > 1:
                    # missing_tasks는 (year, quarter, ...) 튜플 리스트.
                    # 정렬: year 내림차순 -> quarter 내림차순
                    sorted_missing = sorted(missing_tasks, key=lambda x: (x[0], x[1]), reverse=True)
                    
                    print("  🧐 재무제표 종류(연결/별도) 확인 중 (API)...")
                    for t_year, t_quarter, t_report_code, _ in sorted_missing:
                        # 1. 연결 확인
                        cfs_df = get_financial_data(api_key, corp_code, t_year, t_report_code, 'CFS', session)
                        if cfs_df is not None:
                            determined_fs_divs = [('연결', 'CFS')]
                            # 가져온 김에 저장 및 사용
                            save_financial_data_to_db(cfs_df, corp_code, t_year, t_quarter, t_report_code, 'CFS')
                            break # 루프 종료 (확정됨) 
                        
                        # 2. 별도 확인
                        ofs_df = get_financial_data(api_key, corp_code, t_year, t_report_code, 'OFS', session)
                        if ofs_df is not None:
                            determined_fs_divs = [('별도', 'OFS')]
                            # 가져온 김에 저장 및 사용
                            save_financial_data_to_db(ofs_df, corp_code, t_year, t_quarter, t_report_code, 'OFS')
                            break # 루프 종료

                    if len(determined_fs_divs) == 2:
                        print("  ⚠️ 재무제표 종류를 확정하지 못했습니다. 모든 종류를 시도합니다.")

                # 3. 확정된 determined_fs_divs 로 나머지 API 병렬 호출 준비
                api_tasks = []
                for t_year, t_quarter, t_report_code, t_report_name in missing_tasks:
                    # 이미 Probing 단계에서 데이터를 가져왔는데 또 가져오지 않도록 체크해야 함.
                    # (간단하게 구현: Probing에서 가져온 데이터도 다시 가져오더라도 덮어쓰므로 문제는 없지만 비효율적)
                    # -> Probing 때 DB에 저장했으므로, 다시 get_financial_data_from_db 로 확인하면 될까? 
                    # 아니면 그냥 Probing 때 저장만 하고, 여기서 다시 태스크로 넣어서 처리?
                    # -> Probing 때 save_financial_data_to_db 했음.
                    # -> DB를 다시 조회해서 있으면 스킵하는 게 깔끔함.
                    
                    found_after_probing = False
                    for fs_name, fs_code in determined_fs_divs:
                        # Probing 직후 DB 확인
                        db_df_check = get_financial_data_from_db(corp_code, t_year, t_report_code, fs_code)
                        if db_df_check is not None:
                             db_df_check['보고서명'] = t_report_name
                             db_df_check['구분'] = fs_name
                             db_df_check['년도'] = t_year
                             db_df_check['분기'] = t_quarter
                             all_data.append(db_df_check)
                             # print(f"  ✅ {t_year}년 {t_quarter}분기 ({fs_name}) - Probing 중 수집됨")
                             found_after_probing = True
                             break
                    
                    if found_after_probing:
                        continue

                    # 여전히 없으면 API 태스크 추가
                    for fs_name, fs_code in determined_fs_divs:
                        api_tasks.append({
                            'year': t_year,
                            'report_code': t_report_code,
                            'fs_code': fs_code,
                            'report_name': t_report_name,
                            'fs_name': fs_name,
                            'quarter': t_quarter
                        })

                # 병렬 실행
                if api_tasks:
                     with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                        future_to_task = {
                            executor.submit(get_financial_data, api_key, corp_code, t['year'], t['report_code'], t['fs_code'], session): t 
                            for t in api_tasks
                        }
                        
                        for future in concurrent.futures.as_completed(future_to_task):
                            task = future_to_task[future]
                            try:
                                df = future.result()
                                if df is not None:
                                    # DB에 저장
                                    save_financial_data_to_db(df, corp_code, task['year'], task['quarter'], task['report_code'], task['fs_code'])
                                    
                                    # 결과 리스트 절약 (메모리상) -> DB에서 읽는 형태를 취하거나, 그냥 df 사용
                                    # 여기서는 df 직접 사용
                                    df['보고서명'] = task['report_name']
                                    df['구분'] = task['fs_name']
                                    df['년도'] = task['year']
                                    if 'quarter' in task:
                                        df['분기'] = task['quarter']
                                    all_data.append(df)
                                    print(f"  ✅ {task['year']}년 {task['report_name']} ({task['fs_name']}) - API 조회 및 DB 저장")
                                else:
                                    print(f"  ❌ {task['year']}년 {task['report_name']} ({task['fs_name']}) - 데이터 없음")
                            except Exception as exc:
                                print(f"  💥 {task['year']}년 {task['report_name']} 요청 실패: {exc}")


    if not all_data:
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    filtered = combined[['보고서명', '구분', 'account_id', 'account_nm', 'thstrm_amount', '년도']].copy()

    key_items = ['ifrs-full_Revenue', 'dart_OperatingIncomeLoss']
    filtered = filtered[filtered['account_id'].isin(key_items)]

    item_map = {
        'ifrs-full_Revenue': '매출액',
        'dart_OperatingIncomeLoss': '영업이익'
    }
    filtered['항목'] = filtered['account_id'].map(item_map)

    # 보고서명 기준으로 분기 컬럼 추가
    quarter_map = {
        '1분기보고서': 1,
        '반기보고서': 2,
        '3분기보고서': 3,
        '사업보고서': 4
    }
    filtered['분기'] = filtered['보고서명'].map(quarter_map)

    # print("조정전", filtered)

    # Q4 값 조정 적용
    filtered = adjust_q4_values(filtered, year_month)

    # print("조정후", filtered)

    return filtered

def format_display_table(df: pd.DataFrame, corp_code: str, year_month: int = None) -> str:
    """
    수집된 데이터를 보기 좋게 정리된 테이블 형식으로 변환합니다.
    """
    if df.empty:
        return "데이터가 없습니다."

    # 분기 정보가 있으면 분기별로 표시
    if '분기' in df.columns:
        # 분기별 피벗 테이블 생성 (transpose 버전)
        pivot_df = df.pivot_table(
            index=['년도', '분기'],
            columns='항목',
            values='thstrm_amount',
            aggfunc='first'
        )

        # 분기 순서대로 정렬 (과거 분기부터 최신 순)
        unique_years_quarters = sorted(df[['년도', '분기']].drop_duplicates().values.tolist(),
                                     key=lambda x: (x[0], x[1]), reverse=False)

        # 헤더 생성
        header_parts = ['기간', '매출액', '영업이익', '영업이익률']
        
        # 데이터 행 생성
        rows = []
        for year, quarter in unique_years_quarters:
            period_name = f"{year}년 {quarter}분기"
            
            # 값 추출
            rev = pivot_df.loc[(year, quarter), '매출액'] if (year, quarter) in pivot_df.index and '매출액' in pivot_df.columns else None
            op = pivot_df.loc[(year, quarter), '영업이익'] if (year, quarter) in pivot_df.index and '영업이익' in pivot_df.columns else None
            
            # 포맷팅 (백만원 단위)
            rev_str = "-" if pd.isna(rev) or rev is None else "0" if rev == 0 else f"{int(rev / 1000000):,}"
            op_str = "-" if pd.isna(op) or op is None else "0" if op == 0 else f"{int(op / 1000000):,}"
            
            margin = "-"
            if pd.notna(rev) and pd.notna(op) and rev != 0:
                margin = f"{(op / rev) * 100:.2f}"
                
            rows.append([period_name, rev_str, op_str, margin])


        return f"""
    <div style="text-align: right; font-size: 0.9rem; color: #64748b; margin-bottom: 0.5rem;">(단위: 백만원, %)</div>
    <div class="table-container">
        <table>
            <thead>
                <tr>
                    {"".join(f"<th>{col}</th>" for col in header_parts)}
                </tr>
            </thead>
            <tbody>
                {"".join(
                    f"<tr>{''.join(f'<td class=\"number\">{val}</td>' if i > 0 and val != '-' and col_name != '단위' else f'<td>{val}</td>' for i, (col_name, val) in enumerate(zip(header_parts, row_data)))}</tr>"
                    for row_data in rows
                )}
            </tbody>
        </table>
    </div>
    """

    else:
        # 기존 연도별 표시 (변경 없음)
        pivot_df = df.pivot_table(
            index='항목',
            columns='보고서명',
            values='thstrm_amount',
            aggfunc='first'
        )

        # 보고서 순서대로 정렬
        report_order = ['사업보고서', '1분기보고서', '반기보고서', '3분기보고서']
        pivot_df = pivot_df.reindex(columns=report_order, fill_value=None)

        # 연결 데이터优先 처리
        if '구분' in df.columns:
            for item in pivot_df.index:
                item_data = df[df['항목'] == item]
                if not item_data.empty:
                    cfs_data = item_data[item_data['구분'] == '연결']
                    if not cfs_data.empty:
                        for report in report_order:
                            val = cfs_data[cfs_data['보고서명'] == report]['thstrm_amount'].values
                            if len(val) > 0:
                                pivot_df.loc[item, report] = val[0]

        def format_cell(x):
            if pd.isna(x) or x is None:
                return "-"
            elif x == 0:
                return "0"
            else:
                return f"{int(x):,}"

        formatted_df = pivot_df.map(format_cell)

        # 컬럼명에 연월 정보 추가
        report_columns = {}
        for report in report_order:
            report_data = df[df['보고서명'] == report]
            if not report_data.empty:
                latest_year = report_data['년도'].max()
                if report == '사업보고서': month = 12
                elif report == '1분기보고서': month = 3
                elif report == '반기보고서': month = 6
                elif report == '3분기보고서': month = 9
                else: month = 12
                report_columns[report] = f"{latest_year}{month:02d}"
            else:
                report_columns[report] = report

        sorted_columns = sorted(report_columns.items(), key=lambda x: int(x[1]))

        # 헤더 생성
        header_parts = ['항목'] + [col_name for _, col_name in sorted_columns]
        
        # 데이터 행 생성
        rows = []
        for item in formatted_df.index:
            row = formatted_df.loc[item]
            row_vals = [item]
            for report, _ in sorted_columns:
                val = row.get(report, None)
                if pd.isna(val) or val is None: row_vals.append("-")
                elif val == 0: row_vals.append("0")
                else: 
                     # 백만원 단위 변환
                     try:
                        numeric_val = int(str(val).replace(',', ''))
                        row_vals.append(f"{int(numeric_val / 1000000):,}")
                     except:
                        row_vals.append("-")
            rows.append(row_vals)

        # 영업이익률 행 추가
        margin_vals = ['영업이익률']
        for report, _ in sorted_columns:
            try:
                rev = pivot_df.loc['매출액', report]
                op = pivot_df.loc['영업이익', report]
                if pd.notna(rev) and pd.notna(op) and rev != 0:
                    margin = (op / rev) * 100
                    margin_vals.append(f"{margin:.2f}")
                else:
                    margin_vals.append("-")
            except KeyError:
                margin_vals.append("-")
        rows.append(margin_vals)

        return f"""
        <div style="text-align: right; font-size: 0.9rem; color: #64748b; margin-bottom: 0.5rem;">(단위: 백만원, %)</div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        {"".join(f"<th>{col}</th>" for col in header_parts)}
                    </tr>
                </thead>
                <tbody>
                    {"".join(
                        f"<tr>{''.join(f'<td class=\"number\">{val}</td>' if i > 0 and val != '-' else f'<td>{val}</td>' for i, val in enumerate(row_data))}</tr>"
                        for row_data in rows
                    )}
                </tbody>
            </table>
        </div>
        """

def render_page(content: str) -> str:
    return f"""
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>DART 재무정보 검색</title>
        <style>
            :root {{
                --primary: #2563eb;
                --surface: #ffffff;
                --background: #f8fafc;
                --text: #1e293b;
                --border: #e2e8f0;
            }}
            body {{
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
                background-color: var(--background);
                color: var(--text);
                margin: 0;
                padding: 20px;
                line-height: 1.5;
                display: flex;
                flex-direction: column;
                align-items: center;
                min-height: 100vh;
            }}
            .container {{
                width: 100%;
                max-width: 800px;
                background: var(--surface);
                padding: 2rem;
                border-radius: 16px;
                box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1);
            }}
            h1, h2 {{ text-align: center; margin-bottom: 2rem; color: var(--text); }}
            .search-form {{ display: flex; flex-direction: column; gap: 1rem; margin-bottom: 2rem; }}
            input[type="text"] {{
                width: 100%; padding: 12px 16px; border: 1px solid var(--border);
                border-radius: 8px; font-size: 16px; box-sizing: border-box;
            }}
            input[type="text"]:focus {{ outline: none; border-color: var(--primary); }}
            input[type="submit"], .btn {{
                background-color: var(--primary); color: white; border: none;
                padding: 14px; border-radius: 8px; font-size: 16px; font-weight: 600;
                cursor: pointer; width: 100%; text-align: center; text-decoration: none;
                display: inline-block; box-sizing: border-box;
            }}
            .btn-secondary {{ background-color: #64748b; margin-top: 1rem; }}
            /* Table */
            .table-container {{ overflow-x: auto; margin-top: 1rem; border-radius: 8px; border: 1px solid var(--border); }}
            table {{ width: 100%; border-collapse: collapse; font-size: 14px; white-space: nowrap; }}
            th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); }}
            th {{ background-color: #f1f5f9; font-weight: 600; }}
            td.number {{ text-align: right; font-family: "SF Mono", monospace; }}
            /* Loading */
            .overlay {{
                position: fixed; top: 0; left: 0; width: 100%; height: 100%;
                background: rgba(255, 255, 255, 0.9); display: none;
                justify-content: center; align-items: center; z-index: 1000; flex-direction: column;
            }}
            .spinner {{
                width: 40px; height: 40px; border: 4px solid #e2e8f0;
                border-top-color: var(--primary); border-radius: 50%;
                animation: spin 1s linear infinite; margin-bottom: 1rem;
            }}
            @keyframes spin {{ to {{ transform: rotate(360deg); }} }}
            .badge {{
                display: inline-block; padding: 4px 12px; border-radius: 9999px;
                background-color: #e0f2fe; color: #0369a1; font-size: 12px; font-weight: 500; margin-top: 1rem;
            }}
        </style>
        <script>
            function showLoading() {{ document.getElementById('loading-overlay').style.display = 'flex'; }}
        </script>
    </head>
    <body>
        <div class="overlay" id="loading-overlay">
            <div class="spinner"></div>
            <div>데이터 조회 중...</div>
        </div>
        <div class="container">
            {content}
        </div>
    </body>
    </html>
    """


app = FastAPI()

# Render 환경변수에서 API 키를 가져옵니다.
MY_API_KEY = os.getenv("DART_API_KEY")

@app.get("/", response_class=HTMLResponse)
def home():
    content = """
        <h2>DART 재무정보 조회</h2>
        <form action="/search" method="get" class="search-form" onsubmit="showLoading()">
            <label>회사명</label>
            <input type="text" name="company_name" placeholder="예: 삼성전자" required>
            <label>기준 연도(YYYYMM)</label>
            <input type="text" name="year_month" placeholder="예: 202509" value="202509">
            <input type="submit" value="조회하기">
        </form>
    """
    return render_page(content)

@app.get("/search", response_class=HTMLResponse)
def search(company_name: str, year_month: int = 202509):
    start_time = time.time()

    if not MY_API_KEY:
        return render_page(f"<h3>⚠️ 오류</h3><p>DART_API_KEY가 설정되지 않았습니다.</p><a href='/' class='btn btn-secondary'>돌아가기</a>")

    corp_code = search_company_code(MY_API_KEY, company_name)
    if not corp_code:
        return render_page(f"<h3>❌ 검색 실패</h3><p>'{company_name}' 회사를 찾을 수 없습니다.</p><a href='/' class='btn btn-secondary'>돌아가기</a>")

    target_year = year_month // 100
    df = collect_quarterly_financials(MY_API_KEY, corp_code, target_year, year_month)

    if df.empty:
        return render_page(f"<h3>❌ 데이터 없음</h3><p>재무 데이터를 찾을 수 없습니다.</p><a href='/' class='btn btn-secondary'>돌아가기</a>")

    summary_table = format_display_table(df, corp_code, year_month)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    content = f"""
        <h2>'{company_name}' 검색 결과</h2>
        {summary_table}
        <div style="text-align: center; margin-top: 1rem;">
            <span class="badge">⏱️ 처리 시간: {elapsed_time:.2f}초</span>
        </div>
        <a href="/" class="btn btn-secondary">다시 검색하기</a>
    """
    return render_page(content)