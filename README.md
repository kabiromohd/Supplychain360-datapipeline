# Supplychain360-data-platform

## Project overview/background

```SupplyChain360```, a fast-growing retail distribution company in the United States, manages product distribution for hundreds of retail stores across the country.

Over the past year, the company has been experiencing serious operational inefficiencies in its supply chain:

- Stores frequently run out of stock of popular products.
- Warehouses report overstocked inventory for slow-moving items.
- Delivery delays are becoming common.
- Management cannot accurately answer simple questions like:
    - Which products are causing the most stockouts?
    - Which warehouses are inefficient?
    - Which suppliers consistently deliver late?
    
The biggest issue is data fragmentation. Operational data is spread across multiple systems:

- Warehouse inventory systems
- Logistics shipment records
- Supplier delivery logs
- Store sales data
  
Each team manages its own data, and reports are manually compiled in spreadsheets every week. By the time leadership receives insights, the data is already outdated.

The company leadership has decided to build a Unified Supply Chain Data Platform that centralizes operational data and enables efficient analytics to improve:

- Inventory planning
- Supplier performance monitoring
- Shipment tracking
- Demand forecasting

As a Data Engineer hired to design and implement this platform and If successful, the platform will allow the company to reduce stockouts, optimize inventory, and improve delivery efficiency, potentially saving millions of dollars annually.

## Project Overview

This project implements a modern data platform that ingests raw data, loads it into Snowflake, and transforms it into analytics-ready models using dbt Cloud — all orchestrated by Apache Airflow.

The pipeline is designed with production-grade principles:

- Idempotent data loads
- Automated orchestration
- Scalable cloud architecture
- Modular transformations

## Objectives
- Build a reliable ELT pipeline
- Automate workflows using Airflow
- Transform data using dbt Cloud
- Enable analytics use cases such as:
    - Product stockout trends
    - Supplier delivery performance
    - Warehouse efficiency
    - Regional sales demand
    
## Architecture Diagram
<img width="1536" height="1024" alt="ChatGPT Image Apr 3, 2026, 03_39_33 PM" src="https://github.com/user-attachments/assets/205c75c8-a123-4087-9e04-daefd7a9b75f" />


## Tech Stack
```
**Tool**              **Purpose**
Apache Airflow	  Workflow orchestration
Snowflake	      Cloud data warehouse
dbt               Cloud	Data transformation
Python	          Pipeline logic
AWS S3	          Object Data storage
Docker            For containerization
```

## Prerequisites
- Docker Desktop installed and running.
- Terraform installed
- Git installed.
- A Snowflake account and AWS credentials.
- dbt cloud developer account

## Setup Instructions
- Clone Repository
  ```
  git clone https://github.com/kabirmohdo/Supplychain360-data-platform.git
  
  cd Supplychain360-data-platform
  ```

- Install Dependencies
  ```
  pip install -r requirements.txt
  ```
- Start Airflow
  using Docker:
  ```
    docker-compose up -d
  ```

## Airflow Deployment
Check that the containers are running and healthy, access the airflow UI via ```http://localhost:8080```

## Infrastructure
Provision infrastructure with ```terraform```, run the below commands
```
cd terraform-infra

terraform init

terraform plan

terraform apply
```

## Security
Confidentail security information are stored in airflow variable and connection items:

- .env
- snowflake credentails (snowflake_default)
- AWS credentials (aws_default)
- stmp emailing credentials

