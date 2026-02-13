FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default to auto-mode, but allow override
CMD ["python", "main.py", "--auto"]
