FROM python:3.11-slim-buster

WORKDIR /root

COPY Makefile /root/Makefile

#RUN apt update and make
RUN apt-get update && apt-get upgrade -y

RUN apt-get install -y make

# Install Airflow system dependencies: https://airflow.apache.org/docs/apache-airflow/stable/installation.html#system-dependencies
RUN apt-get install -y --no-install-recommends \
    freetds-bin \
    ldap-utils \
    libffi6 \
    libkrb5-dev \
    libsasl2-2 \
    libsasl2-modules \
    libssl1.1 \
    locales  \
    lsb-release \
    sasl2-bin \
    sqlite3 \
    unixodbc

COPY airflow.requirements.txt /root/airflow.requirements.txt

RUN make internal-install-airflow

RUN make internal-install-deps
