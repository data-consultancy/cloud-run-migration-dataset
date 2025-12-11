FROM python:3.11-slim

WORKDIR /app

# instala dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# copia o código (inclui main.py, app/, src/ etc)
COPY . .

# entrypoint do Cloud Run Job
CMD ["python", "src/main.py"]
