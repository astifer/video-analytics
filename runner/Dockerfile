FROM python:3.11-slim

WORKDIR /app

# Install PostgreSQL client
RUN apt-get update && \
    apt-get install -y postgresql-client libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*


COPY requirements.txt .

RUN apt-get update && apt-get install ffmpeg libsm6 libxext6  -y

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
CMD ["uvicorn", "runner:app", "--host", "0.0.0.0", "--port", "7878"]
