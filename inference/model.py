from transformers import YolosImageProcessor, YolosForObjectDetection
from PIL import Image
import torch
from typing import List
from pydantic import BaseModel

model = YolosForObjectDetection.from_pretrained('hustvl/yolos-tiny')
image_processor = YolosImageProcessor.from_pretrained("hustvl/yolos-tiny")

class PredictedObject(BaseModel):
    label: str
    bb: List[int]

class PredictionResult(BaseModel):
    prediction: List[PredictedObject]

async def get_bb(image: Image) -> List[PredictedObject]:
    inputs = image_processor(images=image, return_tensors="pt")
    outputs = model(**inputs)

    # model predicts bounding boxes and corresponding COCO classes
    logits = outputs.logits
    bboxes = outputs.pred_boxes

    # print results
    target_sizes = torch.tensor([image.size[::-1]])
    results = image_processor.post_process_object_detection(outputs, threshold=0.2, target_sizes=target_sizes)[0]
    ress = []
    for score, label, box in zip(results["scores"], results["labels"], results["boxes"]):
        box = [round(i) for i in box.tolist()]
        item = model.config.id2label[label.item()]

        ress.append(PredictedObject(label=item, bb=box))

    return ress