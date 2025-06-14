import numpy as np
from PIL import Image
from transformers import YolosImageProcessor, YolosForObjectDetection
import torch
import cv2

from shared.utils import settings

STREAM_URL = settings.public_urls.get("STREAM_URL", "https://s46.ipcamlive.com/streams/2eulqgccb8zksexmj/stream.m3u8")

# Open the video capture
cap = cv2.VideoCapture(STREAM_URL)

model = YolosForObjectDetection.from_pretrained('hustvl/yolos-tiny')
image_processor = YolosImageProcessor.from_pretrained("hustvl/yolos-tiny")


def get_bb(image: Image):
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

        ress.append({'label': item, 'bb': box})

    return ress


i = 0
if not cap.isOpened():
    print("Error opening video stream")
else:
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            print("Can't receive frame")
            break
        i += 1

        if i % 10 == 0:
            numpy_array = np.array(frame)
            image = Image.fromarray(numpy_array)

            answer_list = get_bb(image)
            # print(numpy_array.shape)
            for i, dct in enumerate(answer_list):

                box = dct['bb']

                x, y, x2, y2 = int(box[0]), int(box[1]), int(box[2]), int(box[3])
                label = dct['label']
                if y > 500:
                    continue
                cv2.rectangle(img=frame, pt1=(x, y), pt2=(x2, y2), color=(255, 0, 0), thickness=2)

                cv2.putText( 
                    img=frame, 
                    text=label, 
                    org=(x, y + 20),
                    fontFace=2,
                    fontScale=0.5,
                    color=(0,0,0),
                    thickness=2
                    )

        cv2.imshow('Frame', frame)
        if cv2.waitKey(1) == ord('q'):
            break
    cap.release()
    # cv2.destroyAllWindows()