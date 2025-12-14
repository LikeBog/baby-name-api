from fastapi import FastAPI

app = FastAPI(title="Baby Name API")

@app.get("/")
def root():
    return {"status": "API running"}

