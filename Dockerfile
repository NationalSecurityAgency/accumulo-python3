FROM python:3.8.17-slim

COPY . /app

RUN pip install /app
