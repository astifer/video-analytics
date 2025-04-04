import cv2
from inference.model import get_bb
import numpy as np
from PIL import Image
STREAM_URL = "https://s35.ipcamlive.com/streams/23fmhujpncmqvpew3/stream.m3u8"

# Open the video capture
cap = cv2.VideoCapture(STREAM_URL)

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