# Superstore Data Pipeline (Apache Airflow with Astro)
=============================================================

# Project Structure

superstore_project/
│── dags/                 
│   │── superstore_ingestion.py  
│   │── superstore_transformation.py  
│
│── include/               
│   │── Superstore.csv
|   │── superstore.db     
│
│── model/
|   │──dim_customers.py
|   │──dim_dates.py
|   │──dim_location.py
|   │──dim_productss.py
|   │──sales_data.py
|
│── plugins/               
│
│── tests/                 
│   │── test_dag_integrity.py
│
│── .env                   
│── airflow_settings.yaml  
│── Dockerfile             
│── requirements.txt       
│── astro.config           
│── README.md              

# Setup Guide

1. Install Astro CLI 
Based on the OS install the Astro CLI below is the documention

"https://www.astronomer.io/docs/astro/cli/overview"

2. Initialize the Astro Project

# astro dev init 
This commond will give the inital setup of Airflow by providing the necessary files and folder setup with example files

3. Install all the dependencies required

Make  sure to get the docker installed locally and running it before using the below commonds

4. Use commonds like

astro dev start  # to start Airflow
astro dev restart # to restart Airflow  
astro dev stop # to stop


# Dataset overview

Metadata
Row ID => Unique ID for each row.
Order ID => Unique Order ID for each Customer.
Order Date => Order Date of the product.
Ship Date => Shipping Date of the Product.
Ship Mode=> Shipping Mode specified by the Customer.
Customer ID => Unique ID to identify each Customer.
Customer Name => Name of the Customer.
Segment => The segment where the Customer belongs.
Country => Country of residence of the Customer.
City => City of residence of of the Customer.
State => State of residence of the Customer.
Postal Code => Postal Code of every Customer.
Region => Region where the Customer belong.
Product ID => Unique ID of the Product.
Category => Category of the product ordered.
Sub-Category => Sub-Category of the product ordered.
Product Name => Name of the Product
Sales => Sales of the Product.
Quantity => Quantity of the Product.
Discount => Discount provided.
Profit => Profit/Loss incurred.


# Architecture

[Data Source] → [Extract] → [Transform] → [Load] → [Data Warehouse] 

* Extract: Reads raw data from sources (CSV files).
* Transform: Data cleaning, removing duplicates, added calculated files and Standardize column names.
* Load: Storing structured data into a SQLite database (used same DB to store raw and transfromed data. Can use a different DB).
* Data Warehouse: A star schema with fact and dimension tables for efficient querying.

<img src="D:\personal projects\airflow local\Airflow-data-pipeline\include\image.png" alt="Database Schema" width="500">




































































Astronomer Cosmos setup and Overview 
========

Welcome to Astronomer! This project was generated after you ran 'astro dev init' using the Astronomer CLI. This readme describes the contents of the project, as well as how to run Apache Airflow on your local machine.

Project Contents
================

Your Astro project contains the following files and folders:

- dags: This folder contains the Python files for your Airflow DAGs. By default, this directory includes one example DAG:
    - `example_astronauts`: This DAG shows a simple ETL pipeline example that queries the list of astronauts currently in space from the Open Notify API and prints a statement for each astronaut. The DAG uses the TaskFlow API to define tasks in Python, and dynamic task mapping to dynamically print a statement for each astronaut. For more on how this DAG works, see our [Getting started tutorial](https://www.astronomer.io/docs/learn/get-started-with-airflow).
- Dockerfile: This file contains a versioned Astro Runtime Docker image that provides a differentiated Airflow experience. If you want to execute other commands or overrides at runtime, specify them here.
- include: This folder contains any additional files that you want to include as part of your project. It is empty by default.
- packages.txt: Install OS-level packages needed for your project by adding them to this file. It is empty by default.
- requirements.txt: Install Python packages needed for your project by adding them to this file. It is empty by default.
- plugins: Add custom or community plugins for your project to this file. It is empty by default.
- airflow_settings.yaml: Use this local-only file to specify Airflow Connections, Variables, and Pools instead of entering them in the Airflow UI as you develop DAGs in this project.

Deploy Your Project Locally
===========================

1. Start Airflow on your local machine by running 'astro dev start'.

This command will spin up 4 Docker containers on your machine, each for a different Airflow component:

- Postgres: Airflow's Metadata Database
- Webserver: The Airflow component responsible for rendering the Airflow UI
- Scheduler: The Airflow component responsible for monitoring and triggering tasks
- Triggerer: The Airflow component responsible for triggering deferred tasks

2. Verify that all 4 Docker containers were created by running 'docker ps'.

Note: Running 'astro dev start' will start your project with the Airflow Webserver exposed at port 8080 and Postgres exposed at port 5432. If you already have either of those ports allocated, you can either [stop your existing Docker containers or change the port](https://www.astronomer.io/docs/astro/cli/troubleshoot-locally#ports-are-not-available-for-my-local-airflow-webserver).

3. Access the Airflow UI for your local Airflow project. To do so, go to http://localhost:8080/ and log in with 'admin' for both your Username and Password.

You should also be able to access your Postgres Database at 'localhost:5432/postgres'.

Deploy Your Project to Astronomer
=================================

If you have an Astronomer account, pushing code to a Deployment on Astronomer is simple. For deploying instructions, refer to Astronomer documentation: https://www.astronomer.io/docs/astro/deploy-code/

Contact
=======

The Astronomer CLI is maintained with love by the Astronomer team. To report a bug or suggest a change, reach out to our support.
