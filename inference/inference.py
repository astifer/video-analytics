from fastapi import FastAPI, File, UploadFile, HTTPException
from model import get_bb, PredictionResult
from PIL import Image

import io

app = FastAPI(title="Inference Service")

@app.post("/inference/")
async def run_inference(file: UploadFile = File(...)) -> PredictionResult:
    try:
        contents = await file.read()
        image = Image.open(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid image file")
    
    predictions = await get_bb(image)
    return PredictionResult(prediction=predictions)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("inference_service:app", host="0.0.0.0", port=8001, reload=True)
