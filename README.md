# Ecommerce ETL Pipeline

My self-learning journey into **Data Engineering**, where I explore different tools and techniques to build an end-to-end ETL pipeline.

## Part 1: ETL with Pandas & Docker
A basic ETL pipeline using **Pandas** to process ecommerce sales data and load it into a **PostgreSQL** database. This is containerized using **Docker**, and **pgAdmin** is included for database inspection.

### Data Ingestion Workflow

- 🔹 **Dockerized PostgreSQL**  
  A Postgres container stores structured ecommerce data.

- 🔹 **ETL with Pandas**  
  A Python script reads and cleans a large CSV file in chunks, transforms it using `pandas`, and loads it into the Postgres database.

- 🔹 **Connect pgAdmin & Postgres**  
  pgAdmin to query Postgres db.

- 🔹 **Dockerized Ingestion Script**  
  The ingestion script is containerized and can be run with arguments to perform ingestion from CSV to PostgreSQL.

### Set Up

#### Step 1: Start Postgres and pgAdmin Containers

```bash
docker network create pg-network

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ecommerce_sales" \
  -v $(pwd)/ecommerce_sales_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13

docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

#### Step 2: Build Docker Image for Ingestion
```bash
docker build -t ecommerce_ingest:v001 .
```

#### Step 3: Run Ingestion Script
```bash
docker run -v $(pwd)/ecommerce_sales.csv:/app/ecommerce_sales.csv \
  --network=pg-network \
  ecommerce_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ecommerce_sales \
    --table_name=ecommerce_sales_data
```

## Part 2: with pySpark for Batch Processing

## Part 3: with Kafka for Streaming

## Part 4: Data Visualisation with Streamlit