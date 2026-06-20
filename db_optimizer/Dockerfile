# syntax=docker/dockerfile:1

FROM python:3.12-alpine

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY ./src/ .

CMD python3  app.py