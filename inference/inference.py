from fastapi import FastAPI, File, UploadFile, HTTPException
from model import get_bb, PredictionResult
from PIL import Image

import io

app = FastAPI(title="Inference Service")

@app.post("/inference/")
async def run_inference(file: UploadFile = File(...)) -> PredictionResult:
    print("new request")
    try:
        print("try to read file..")
        contents = await file.read()
        print("convert into PIL image")
        image = Image.open(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid image file")
    
    print("Getting predictions")
    predictions = await get_bb(image)
    print("Succesfully get prediction!")
    return PredictionResult(prediction=predictions)
