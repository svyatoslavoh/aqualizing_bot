FROM python:3.9
ADD . /app/bot

RUN cd /app/bot; make first-install

WORKDIR /app/bot
