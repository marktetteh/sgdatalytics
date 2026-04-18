FROM python:3.11-slim

WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ .
COPY database/ ./database/

EXPOSE 8000

CMD python3 database/init_db.py && gunicorn -w 2 -b 0.0.0.0:$PORT api:app
