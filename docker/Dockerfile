FROM python:3.11-slim

WORKDIR /opt/project

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /opt/project

CMD ["python", "-m", "src.ingestion.run", "--config", "src/config/pipeline.yaml"]

