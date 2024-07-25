FROM python:3.9.19-alpine3.20
WORKDIR /app
COPY *requirements* /app/
RUN apk upgrade --update-cache --available && \
    apk add --no-cache librdkafka librdkafka-dev && \
    apk add --no-cache alpine-sdk python3-dev
RUN pip install -r requirements.txt
RUN pip install -r requirements_test.txt
COPY . /app
RUN pip install -e .

