FROM --platform=linux/amd64 python:3.11-slim-bullseye

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD ["python", "spark.py"]