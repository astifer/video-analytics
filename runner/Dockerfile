FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y
RUN pip install --no-cache-dir -r requirements.txt


COPY . .

CMD ["uvicorn", "runner:app", "--host", "0.0.0.0", "--port", "7878"]
