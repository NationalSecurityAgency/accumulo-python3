FROM python:3.8.17-slim

LABEL org.opencontainers.image.source=https://github.com/NationalSecurityAgency/accumulo-python3

COPY . /app

RUN pip install /app
