FROM python:3.11-slim

WORKDIR /app

# Sem buffer no stdout: se o container morrer (ex.: OOM), os logs até ali não se perdem.
ENV PYTHONUNBUFFERED=1

COPY app/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

CMD ["python", "app/src/main.py"]