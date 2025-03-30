import cv2
import requests
from PIL import Image
import io
import numpy as np
import time

STREAM_URL = "https://s35.ipcamlive.com/streams/23fmhujpncmqvpew3/stream.m3u8"

def send_frame_to_inference(frame):

    rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
    pil_image = Image.fromarray(rgb_frame)
    

    buf = io.BytesIO()
    pil_image.save(buf, format="JPEG")
    buf.seek(0)
    
    files = {'file': ("frame.jpg", buf, "image/jpeg")}

    response = requests.post("http://inference_service:8001/inference/", files=files)
    if response.status_code == 200:
        return response.json()
    else:
        return None

def run():

    cap = cv2.VideoCapture(STREAM_URL)
    i = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
        i += 1
        if i % 10 == 0:
            result = send_frame_to_inference(frame)
            if result:

                for dct in result["predictions"]:
                    box = dct['bb']
                    x, y, x2, y2 = map(int, box)
                    label = dct['label']

                    cv2.rectangle(frame, (x, y), (x2, y2), (255, 0, 0), 2)
                    cv2.putText(frame, label, (x, y + 20),
                                cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 2)
                    
        # send frame to the needed service 
        if cv2.waitKey(1) == ord('q'):
            break
        
    cap.release()
    cv2.destroyAllWindows()

if __name__ == '__main__':
    while True:
        print('Im alive')
        time.sleep(5)