FROM python:3.11

RUN apt-get update && apt-get install -qq --no-install-recommends curl
COPY requirements.txt /

RUN pip install -r requirements.txt

WORKDIR /app