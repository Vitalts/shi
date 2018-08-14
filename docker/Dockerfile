FROM python:2.7-alpine

RUN apk update && \
    apk add --virtual build-deps gcc python-dev musl-dev && \
    apk add postgresql-dev && \
    apk add wget unzip

RUN pip install psycopg2

RUN wget -P /tmp https://github.com/Vitalts/shi/archive/master.zip && \
    unzip -d /app /tmp/master.zip && \
    rm /tmp/master.zip

ENTRYPOINT python /app/shi-master/server.py
