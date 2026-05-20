FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Komenda, która odpali się po starcie kontenera
CMD ["python", "generator.py"]