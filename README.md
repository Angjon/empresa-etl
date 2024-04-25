# Empresa-ETL

## Table of Contents
- [Objective](#Objective)
- [Get started](#Get-started)
- [Process](#Process)
- [Improvements](#Improvements)
- [Author](#Author)

## Objetive
Create an ETL process based on a real world scenario, extracting Brazillian companies data provided by official governmental source, using Airflow, Docker and Python.

## Get started
Make sure you have installed docker and docker composer in your computer.

Clone the directory:
```bash
git clone https://github.com/Angjon/empresa-etl.git
```

Cd into airflow folder:
```bash
cd .\airflow\
```
Run docker:
```bash
docker-compose up
```

## Process
- Extract data from a raw source, in this case the following URL: [https://dados.rfb.gov.br/CNPJ](https://dados.rfb.gov.br/CNPJ/)
- Transform the data following a medallion architecture (raw -> bronze -> silver -> gold)
- Dump the data in a parquet format
- Orquestrate all the scripts with Airflow in a docker container

## Improvements
Load the data into a relational database or a NoSQL (probably will do it for next project).
Create a poke script to check when the source has made any change to the raw file, if so, execute the necessary scripts.
Maybe breakdown the scripts into smaller parts, would be easier to debug it throught the airflow UI.

## Author
Jonas Angulski