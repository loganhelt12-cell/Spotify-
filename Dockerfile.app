
FROM python:3.11-slim

# system deps for ffmpeg, chromaprint tooling and general tools
RUN apt-get update && apt-get install -y build-essential ffmpeg libchromaprint-tools     && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app
EXPOSE 8000

CMD ["uvicorn", "fastapi_main:app", "--host", "0.0.0.0", "--port", "8000"]
