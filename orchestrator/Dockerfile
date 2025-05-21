FROM python:3.11-slim

WORKDIR /app

# Install PostgreSQL client
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY init_db.sql /docker-entrypoint-initdb.d/

# Wait for database to be ready and initialize tables
COPY wait-for-it.sh /wait-for-it.sh

RUN chmod +x /wait-for-it.sh

# Use uvicorn to run the FastAPI application
CMD ["/wait-for-it.sh", "pg_orchestrator", "5432", "uvicorn", "orchestrator:app", "--host", "0.0.0.0", "--port", "1612"]