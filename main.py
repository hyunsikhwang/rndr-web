from fastapi import FastAPI
import os

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Hello World! This is FastAPI on Render."}

if __name__ == "__main__":
    # Render는 환경 변수로 PORT를 지정해주므로 이를 읽어와야 합니다.
    port = int(os.environ.get("PORT", 8000))
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=port)