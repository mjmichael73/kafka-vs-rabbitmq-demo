FROM python:3.15.1-slim

WORKDIR /app
ENV PYTHONPATH="/app/src"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

CMD ["python", "-m", "compare_demo"]
