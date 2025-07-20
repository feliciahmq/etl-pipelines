# Ecommerce ETL Pipeline

My self-learning journey into **Data Engineering**, where I explore different tools and techniques to build an end-to-end ETL pipeline.

dataset from [here](https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data?resource=download)

## Part 1: ETL with Pandas & Docker
A basic ETL pipeline using **Pandas** to process ecommerce sales data and load it into a **PostgreSQL** database. This is containerized using **Docker**, and **pgAdmin** is included for database inspection.

### Data Ingestion Workflow

- 🔹 **Dockerized PostgreSQL**  
  A Postgres container stores structured ecommerce data.

- 🔹 **ETL with Pandas**  
  A Python script reads and cleans a large CSV file in chunks, transforms it using `pandas`, and loads it into the Postgres database.

- 🔹 **Connect pgAdmin & Postgres**  
  pgAdmin to query Postgres db.

- 🔹 **Ingestion Script**  
  The ingestion script is can be run with arguments to perform ingestion from CSV to PostgreSQL.

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
Used Apache Spark (PySpark) to clean, transform, and aggregate large e-commerce datasets for analysis and dashboarding.

### Steps:
- Load and clean multiple CSV files (e.g., sales, pricing, international).
- Normalize datatypes, handle nulls and malformed values.
- Extract time-based columns (e.g., Month from Date).
- Create temporary views for SQL-based querying.
- Write intermediate outputs to Parquet files for efficient access for streamlit dashboard.

### ETL 

| Step       | Description                                                                 |
|------------|-----------------------------------------------------------------------------|
| **Extract**   | Ingests raw CSVs such as `ecommerce_sales.csv`, `international_sales.csv` and `may-2022.csv`from local. |
| **Transform** | Cleans and processes the data using PySpark: removes malformed entries, casts datatypes, filters nulls, and standardizes fields like `Date`, `Amount`, `SKU`, and `Category`. |
| **Load**      | Writes transformed datasets and aggregated results to the `outputs/` folder in Parquet format. |

### Data Modeling: Facts & Dimensions

The ETL output follows a basic star schema design:

- **Fact Tables:** e.g.
  - `monthly_sales_by_category`: Monthly revenue by product style (category).
  - `top_product_sku_by_amount`: Top-selling SKUs by total revenue.
  - `monthly_sales_by_customer`: Gross sales grouped by international customers and month.
  - `avg_price_per_sku`: Aggregated average pricing across platforms for each SKU.

- **Dimension Tables:**
  - `sales`: Cleaned local e-commerce sales data (Amazon).
  - `international_sales`: Cleaned export/international order data.
  - `pricing`: Cross-channel pricing info from `may-2022.csv`.

## Part 3: Data Visualisation with Streamlit
Visualize the aggregated data using Streamlit to explore insights across local sales, international performance, and pricing.

### Key Features:
- Tabbed dashboard for:
  - Local sales (monthly trends, top SKUs, sales by fulfilments)
  - International sales (customer-wise revenue, top SKUs)
  - Pricing comparison (pricing across platforms, avg price per SKUs)

### How It Works:
- Loads processed Parquet outputs from Part 2.
- Converts results to Pandas dataframe for visualization.

### ▶️ To Run:
```bash
cd part2
streamlit run streamlit_app.py
```

## will try kafka someday
generated readme with chatgpt :D
