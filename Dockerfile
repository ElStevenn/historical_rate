FROM python:3.12-slim

WORKDIR /

RUN pip install --no-cache-dir fastapi aiohttp uvicorn pydantic requests schedule apscheduler pytz numpy pandas python-dotenv redis matplotlib websockets python-dotenv

COPY . .

EXPOSE 80 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]