# Realtime API DATA Streaming | Holistic Data Engineering Project

## Table of Contents
- [Introduction](#introduction)
- [Solution Architecture](#solution-architecture)
- [What You'll Learn](#what-youll-learn)
- [Technologies](#technologies)
- [Getting Started](#getting-started)

## Introduction

This project gives out as a component guide for building an end-to-end data engineering pipeline for newbies. It encompasses each stage, from data ingestion and processing to storage, employing a robust tech stack that includes Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and fast databases like MySQL, Redshift, etc. All components are containerized using Docker for ease of deployment and scalability.

## Solution Architecture

![Solution Architecture](https://github.com/awaisajaz1/air_savant/blob/main/DE%20Architecture.png)


The project is designed with the following tools:

- **Language**: We use Python above 3.9, we would need walrus operators to pick chunk of data in smaller server configurations.
- **Environment**: containerization with python virtual environment is a old love that help us to avoid any dependencies conflicts with base operating system.
- **Data Source**: We use `randomuser.me` API to generate random user data for our pipeline.
- **Apache Airflow**: Responsible for orchestrating the pipeline and storing fetched data in a PostgreSQL/MYSQL etc. databases.
- **Apache Kafka and Zookeeper**: streaming data from PostgreSQL to the processing engine like spark, nifi etc.
- **Control Center and Schema Registry**: schema management of our Kafka streams.
- **Apache Spark**: For data processing with its master and worker nodes.
- **MYSQL**: Where the curated data will be stored.
- **Visualization**: You can pick tool to start with but my recommendation will be either use ***apache superset*** or ***metabase***, both are open source and easy to deploy in anny machine.

## What You'll Learn

- Setting up a data pipeline with Apache Airflow
- Real-time data streaming with Apache Kafka
- Distributed synchronization with Apache Zookeeper
- Basic Data processing techniques with Apache Spark
- Data storage solutions with MYSQL, PostgreSQL etc.
- Containerizing your entire data engineering setup with Docker

## Technologies

- Apache Airflow
- Python
- Apache Kafka
- Apache Zookeeper
- Apache Spark
- MYSQL (You can use a tool of your choice to set up a target container)
- PostgreSQL
- Docker

## Getting Started


1. Using virtual environment  
   To create a virtual environment in the virtual_env folder:
   ```
   python -m venv venv
   ```
   
2. To activate the virtual environment
   ```
   .\venv\Scripts\activate
   ```

3. With the virtual environment activated we can install the packages in the requirements.txt file:
   ```
   pip install -r requirements.txt
   ```

4. Clone the repository:
    ```bash
    git clone git@github.com:awaisajaz1/air_savant.git
    ```

5. Navigate to the project directory:
    ```bash
    cd air_savant
    ```

6. Run Docker Compose to spin up the services:
    ```bash
    docker-compose up
    ```
7. List all containers:
   ```bash
   docker container ls -a
   ```
