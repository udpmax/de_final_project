# NAYA College Data Engineer Final Project - Maxim Dahav

## Name
ETL for TLV airport

## Description

- Final project for the data engineering course that will include the topics learned during the 6 months long course.
- The purpose of the project is to create a data ETL pipeline from beginning to end that will be a close approximation to a real-world architecture.
- Data set used: TLV Flights (Departure and landing) that is updated every 15 minutes: https://data.gov.il/dataset/flydata
- Technology stack: Python, Pandas, GCP, Amazon Redshift, MongoDB Atlas, Amazon S3, Apache Airflow, Apache Kafk, Docker.

## Project Drawing

![alt text](https://github.com/udpmax/de_final_project/blob/main/Proj_draw.jpg?raw=true "ETL for TLV Flights")

## Project Flow

### Extract

- Use python code to extract json data from API in Batch mode – TBD and load the data to kafka topic 
- Use Python code to load a csv file to s3, convert it to json and load to kafka topic

### Transform

Consume the kafka data and transform, clean and enrich data and save the ‘clean’ data in new csv file using pandas package in Python 

### Load

- Load the clean data to Amazon redshift using the built-in copy from S3 Redshift functionality
- Load  part of the data in operational database (noSQL) - On time ELAL Flight taking off and landing in TLV  – Will be done with MongoDB Atlas

### Orchestration and management

We will use GCP VM instance to deploy Docker compose and run the project, while the timing and orchestration will be done the Apache Airflow

## Execution

1. Navigate to /airflow 
2. Run 'docker compose --build'
3. Navigate to airflow url gcp_ip:8080/dags/final_proj_test
4. Run the dag to execture the ETL.
5. Follow new entries results in MongoDB Atlas and Amazon Redshift
6. Alternatively the flow can be executed with running 'python execution_flow.py' in /airflow/python_code/

