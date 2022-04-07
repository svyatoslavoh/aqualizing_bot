FROM python:3.9
ADD . /app/bot

RUN cd /app/bot; make first-install;

WORKDIR /usr/lib/oracle/21/
USER root

RUN apt-key adv --recv-keys --keyserver keyserver.ubuntu.com 467B942D3A79BD29
RUN apt-get update && apt-get install libaio1 && apt-get install wget -y && apt-get install alien -y
RUN wget -qO- -O instantclient_21_4.rpm https://download.oracle.com/otn_software/linux/instantclient/214000/oracle-instantclient-basic-21.4.0.0.0-1.el8.x86_64.rpm &&  alien -i  instantclient_21_4.rpm 
RUN sh -c "echo /usr/lib/oracle/21/client64/lib > /etc/ld.so.conf.d/oracle-instantclient.conf"
RUN ldconfig

WORKDIR /app/bot
