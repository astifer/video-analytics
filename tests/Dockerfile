FROM python:3.11-slim

WORKDIR /app

RUN apt-get update 

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["pytest", "./test_pipeline.py"]