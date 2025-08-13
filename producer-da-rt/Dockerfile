FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer_da_rt.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "producer_da_rt.py"]
