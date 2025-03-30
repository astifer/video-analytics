from fastapi import FastAPI

app = FastAPI(title="Orchestrator")

@app.get("/status")
async def status():
    return {"status": 200}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("orchestrator:app", host="0.0.0.0", port=1612, reload=True)