from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse
import os
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import pandas as pd
from typing import Optional, Dict, List  # 이 줄을 추가하세요!

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

def get_financial_data(api_key: str, corp_code: str, year: int, report_type: str, fs_div: str) -> Optional[pd.DataFrame]:
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

        print(f"\n🔄 [{year_month} 기준] {corp_code} 재무데이터 수집 시작...")
        print(f"   대상 분기: {quarters_to_collect}")

        for target_year, target_quarter in quarters_to_collect:
            if target_quarter == 1:
                report_name = '1분기보고서'
                report_code = '11013'
            elif target_quarter == 2:
                report_name = '반기보고서'
                report_code = '11012'
            elif target_quarter == 3:
                report_name = '3분기보고서'
                report_code = '11014'
            else:  # target_quarter == 4
                report_name = '사업보고서'
                report_code = '11011'

            for fs_name, fs_code in fs_divs:
                df = get_financial_data(api_key, corp_code, target_year, report_code, fs_code)

                if df is not None:
                    df['보고서명'] = report_name
                    df['구분'] = fs_name
                    df['년도'] = target_year
                    df['분기'] = target_quarter
                    all_data.append(df)
                    print(f"  ✅ {target_year}년 {report_name} ({fs_name})")
                else:
                    print(f"  ❌ {target_year}년 {report_name} ({fs_name}) - 데이터 없음")
    else:
        # 기존 연도 처리
        print(f"\n🔄 [{year}년] {corp_code} 재무데이터 수집 시작...")

        for report_name, report_code in report_types:
            for fs_name, fs_code in fs_divs:
                df = get_financial_data(api_key, corp_code, year, report_code, fs_code)

                if df is not None:
                    df['보고서명'] = report_name
                    df['구분'] = fs_name
                    df['년도'] = year
                    all_data.append(df)
                    print(f"  ✅ {report_name} ({fs_name})")
                else:
                    print(f"  ❌ {report_name} ({fs_name}) - 데이터 없음")

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

        # 헤더 및 테이블 생성
        lines = []
        lines.append(" " * 25 + "📋 [재무 정보 요약 테이블]")
        lines.append("=" * 80)

        # 헤더 생성 (동적 컬럼 수)
        header_parts = ['기간']
        for item in ['매출액', '영업이익', '영업이익률']:
            header_parts.append(item)
        header_parts.append('단위')

        header = " | ".join([f"{header_parts[0]:<12}" if i == 0 else
                             f"{col:>12}" if i == len(header_parts)-1 else
                             f"{col:>10}" for i, col in enumerate(header_parts)])
        lines.append(header)
        lines.append("-" * 80)

        # 데이터 행 생성
        for year, quarter in unique_years_quarters:
            period_name = f"{year}년 {quarter}분기"
            row_parts = [f"{period_name:<12}"]

            # 매출액
            rev = pivot_df.loc[(year, quarter), '매출액'] if (year, quarter) in pivot_df.index and '매출액' in pivot_df.columns else None
            if pd.isna(rev) or rev is None:
                row_parts.append("-")
            elif rev == 0:
                row_parts.append("0")
            else:
                row_parts.append(f"{int(rev):,}")

            # 영업이익
            op = pivot_df.loc[(year, quarter), '영업이익'] if (year, quarter) in pivot_df.index and '영업이익' in pivot_df.columns else None
            if pd.isna(op) or op is None:
                row_parts.append("-")
            elif op == 0:
                row_parts.append("0")
            else:
                row_parts.append(f"{int(op):,}")

            # 영업이익률 계산
            if pd.notna(rev) and pd.notna(op) and rev != 0:
                margin = (op / rev) * 100
                row_parts.append(f"{margin:.2f}")
            else:
                row_parts.append("-")

            row_parts.append("원")
            row_str = " | ".join([f"{row_parts[0]:<12}" if i == 0 else
                                 f"{val:>12}" if i == len(row_parts)-1 else
                                 f"{val:>10}" for i, val in enumerate(row_parts)])
            lines.append(row_str)

        lines.append("=" * 80)

        return "\n".join(lines)

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

        lines = []
        lines.append(" " * 25 + "📋 [재무 정보 요약 테이블]")
        lines.append("=" * 80)

        # 컬럼명에 연월 정보 추가
        # 보고서별로 연월 정보를 추출하여 컬럼명에 추가
        report_columns = {}
        for report in report_order:
            report_data = df[df['보고서명'] == report]
            if not report_data.empty:
                # 가장 최근 연도를 사용
                latest_year = report_data['년도'].max()
                # 보고서 유형에 따라 월 결정
                if report == '사업보고서':
                    month = 12
                elif report == '1분기보고서':
                    month = 3
                elif report == '반기보고서':
                    month = 6
                elif report == '3분기보고서':
                    month = 9
                else:
                    month = 12
                # 컬럼명을 연월(YYYYMM) 기준으로만 표시
                report_columns[report] = f"{latest_year}{month:02d}"
            else:
                report_columns[report] = report

        # 과거->최신 순으로 컬럼 순서 재배치
        # 연월(YYYYMM) 기준으로 정렬
        sorted_columns = sorted(report_columns.items(), key=lambda x: int(x[1]))

        # 헤더 생성
        header_parts = ['항목']
        for report, col_name in sorted_columns:
            header_parts.append(col_name)
        header_parts.append('단위')

        header = " | ".join([f"{header_parts[0]:<12}" if i == 0 else
                             f"{col:>12}" if i == len(header_parts)-1 else
                             f"{col:>10}" for i, col in enumerate(header_parts)])
        lines.append(header)
        lines.append("-" * 80)

        # 데이터 행 생성
        for item in formatted_df.index:
            row_parts = [f"{item:<12}"]
            row = formatted_df.loc[item]

            for report, col_name in sorted_columns:
                val = row.get(report, None)
                if pd.isna(val) or val is None:
                    row_parts.append("-")
                elif val == 0:
                    row_parts.append("0")
                else:
                    row_parts.append(f"{int(str(val).replace(',', '')):,}")

            row_parts.append("원")
            row_str = " | ".join([f"{row_parts[0]:<12}" if i == 0 else
                                 f"{val:>12}" if i == len(row_parts)-1 else
                                 f"{val:>10}" for i, val in enumerate(row_parts)])
            lines.append(row_str)

        lines.append("-" * 80)

        # 영업이익률 행 추가 (분기별 계산)
        margin_parts = ['영업이익률']
        for report, col_name in sorted_columns:
            try:
                rev = pivot_df.loc['매출액', report]
                op = pivot_df.loc['영업이익', report]
                if pd.notna(rev) and pd.notna(op) and rev != 0:
                    margin = (op / rev) * 100
                    margin_parts.append(f"{margin:.2f}")
                else:
                    margin_parts.append("-")
            except KeyError:
                margin_parts.append("-")

        margin_parts.append("%")
        margin_str = " | ".join([f"{margin_parts[0]:<12}" if i == 0 else
                                f"{val:>12}" if i == len(margin_parts)-1 else
                                f"{val:>10}" for i, val in enumerate(margin_parts)])
        lines.append(margin_str)

        lines.append("=" * 80)

        return "\n".join(lines)


app = FastAPI()

# Render 환경변수에서 API 키를 가져옵니다.
MY_API_KEY = os.getenv("DART_API_KEY")

@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <html>
        <body>
            <h2>DART 재무정보 조회 (FastAPI)</h2>
            <form action="/search" method="get">
                회사명: <input type="text" name="company_name">
                연도(YYYYMM): <input type="text" name="year_month" placeholder="202509">
                <input type="submit" value="조회하기">
            </form>
        </body>
    </html>
    """

@app.get("/search")
def search(company_name: str, year_month: int = 202509):
    if not MY_API_KEY:
        return {"error": "DART_API_KEY가 설정되지 않았습니다."}

    # 1. 회사 코드 검색
    corp_code = search_company_code(MY_API_KEY, company_name)
    if not corp_code:
        return {"error": f"'{company_name}' 회사를 찾을 수 없습니다."}

    # 2. 재무데이터 수집 (기존 로직 사용)
    target_year = year_month // 100
    df = collect_quarterly_financials(MY_API_KEY, corp_code, target_year, year_month)

    if df.empty:
        return {"error": "데이터를 찾을 수 없습니다."}

    # 3. 테이블 변환 및 출력
    summary_table = format_display_table(df, corp_code, year_month)
    
    # 웹 브라우저에서 보기 좋게 <pre> 태그로 감싸서 반환
    return HTMLResponse(content=f"<h3>{company_name} 검색 결과</h3><pre>{summary_table}</pre>")