#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pyspark
from pyspark.sql import SparkSession


# In[3]:


try:
    spark.stop()
except NameError:
    pass  # Spark not defined yet, safe to ignore

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("EcommerceETL") \
    .getOrCreate()


# In[4]:


df_sales_raw = spark.read.csv("../ecommerce_sales.csv", header=True)


# In[5]:


from pyspark.sql.functions import col, count, when

df_sales_raw.select([count(when(col(c).isNull(), c)).alias(c) for c in df_sales_raw.columns]).show()


# In[6]:


from pyspark.sql.functions import col, to_date, when, regexp_replace, expr

df_sales = df_sales_raw.select(
    to_date(col("Date"), "MM-dd-yy").alias("Date"),
    col("Status"),
    col("Fulfilment"),
    when(col("Style").isNull() | (col("Style") == ""), "Unknown").otherwise(col("Style")).alias("Style"),
    when(col("SKU").isNull() | (col("SKU") == ""), "Unknown").otherwise(col("SKU")).alias("SKU"),
    col("ASIN"),
    when(col("Courier Status").isNull() | (col("Courier Status") == ""), "Unknown").otherwise(col("Style")).alias("Courier_Status"),
    col("Qty").cast("float").cast("int"),
    col("Amount").cast("float"),
    col("B2B").cast("boolean"),
    col("currency").alias("Currency")
).filter(col("Amount").isNotNull() & col("Qty").isNotNull())


# In[7]:


df_sales.printSchema()


# In[8]:


df_sales.show(5)


# In[9]:


df_sales.select([count(when(col(c).isNull(), c)).alias(c) for c in df_sales.columns]).show()


# In[10]:


df_international_raw = spark.read.csv("../international_sales.csv", header=True)


# In[11]:


df_international_raw.select([count(when(col(c).isNull(), c)).alias(c) for c in df_international_raw.columns]).show()


# In[12]:


df_cleaned_raw = df_international_raw.withColumn(
    "GROSS_AMT_CLEAN", regexp_replace(col("GROSS AMT"), ",", "")
)

df_valid_dates = df_cleaned_raw.filter(col("DATE").rlike("^[0-9]{2}-[0-9]{2}-[0-9]{2}$"))

df_international = df_valid_dates.select(
    to_date(col("DATE"), "MM-dd-yy").alias("Date"), 
    col("Months").alias("Month"),
    col("CUSTOMER"),
    when(col("Style").isNull() | (col("Style") == ""), "Unknown").otherwise(col("Style")).alias("Style"),
    when(col("SKU").isNull() | (col("SKU") == ""), "Unknown").otherwise(col("SKU")).alias("SKU"),
    when(col("Size").isNull() | (col("Size") == ""), "Unknown").otherwise(col("Size")).alias("Size"),
    col("PCS").cast("float").cast("int").alias("Pieces"),
    col("RATE").cast("float").alias("Rate"),
    expr("try_cast(GROSS_AMT_CLEAN as float)").alias("Gross_Amount")
)

df_international = df_international.filter(col("Gross_Amount").isNotNull())


# In[13]:


df_international.printSchema()


# In[14]:


df_international.select([count(when(col(c).isNull(), c)).alias(c) for c in df_international.columns]).show()


# In[15]:


df_pricing_raw = spark.read.csv("../may-2022.csv", header=True)


# In[16]:


df_pricing_raw.select([count(when(col(c).isNull(), c)).alias(c) for c in df_pricing_raw.columns]).show()
df_pricing_raw.printSchema()


# In[17]:


int_columns = [
    "Weight", "TP", "MRP Old", "Final MRP Old", "Ajio MRP", "Amazon MRP", "Amazon FBA MRP",
    "Flipkart MRP", "Limeroad MRP", "Myntra MRP", "Paytm MRP", "Snapdeal MRP"
]

df_pricing = df_pricing_raw.select(
    col("index"),
    col("Sku"),
    col("Catalog"),
    col("Category"),
    *[
        when(
            regexp_replace(col(c), ",", "").rlike("^[0-9]+$"),
            regexp_replace(col(c), ",", "").cast("int")
        ).otherwise(None).alias(c) if c in int_columns else col(c)
        for c in df_pricing_raw.columns if c not in ["index", "Sku", "Catalog", "Category"]
    ]
)


# In[18]:


df_pricing.printSchema()


# In[55]:


from pyspark.sql.functions import date_format
df_sales = df_sales.withColumn("Month", date_format("Date", "yyyy-MM"))


# In[56]:


df_sales.createOrReplaceTempView("sales")
df_international.createOrReplaceTempView("international_sales")
df_pricing.createOrReplaceTempView("pricing")


# In[57]:


print(df_sales.columns)


# In[63]:


df_sales.printSchema()


# In[64]:


print(df_international.columns)


# In[65]:


print(df_pricing.columns)


# In[66]:


spark.sql("""
CREATE OR REPLACE TEMP VIEW top_categories AS
SELECT Style AS Category
FROM sales
GROUP BY Style
ORDER BY SUM(Amount) DESC
LIMIT 5
""")

monthly_sales_by_category = spark.sql("""
SELECT 
  Month, 
  Style AS Category, 
  SUM(Amount) AS Revenue
FROM sales
WHERE Style IN (SELECT Category FROM top_categories)
  AND Month IS NOT NULL
GROUP BY Month, Style
ORDER BY Month, Category
""")
# line chart
monthly_sales_by_category = monthly_sales_by_category.withColumn("Month", to_date("Month", "yyyy-MM"))


# In[67]:


monthly_sales_by_category.printSchema()


# In[68]:


monthly_sales_by_category.show(10, truncate=False)
monthly_sales_by_category.write.mode("overwrite").parquet("outputs/monthly_sales_by_category")


# In[69]:


top_product_sku_by_amount = spark.sql("""
SELECT SKU, SUM(Amount) AS Total_Gross, COUNT(*) AS Orders
FROM sales
GROUP BY SKU
ORDER BY Total_Gross DESC
LIMIT 10    
""")
# bar chart


# In[70]:


top_product_sku_by_amount.show(10, truncate=False)
top_product_sku_by_amount.write.mode("overwrite").parquet("outputs/top_product_sku_by_amount")


# In[71]:


sales_by_fulfilment = spark.sql("""
SELECT Fulfilment, SUM(Amount) AS Revenue, COUNT(*) AS Orders
FROM sales
GROUP BY Fulfilment
""")
# pie chart


# In[72]:


sales_by_fulfilment.show(10, truncate=False)
sales_by_fulfilment.write.mode("overwrite").parquet("outputs/sales_by_fulfilment")


# In[73]:


monthly_sales_by_customer = spark.sql("""
SELECT Month, CUSTOMER, SUM(Gross_Amount) AS Revenue
FROM international_sales
GROUP BY Month, CUSTOMER
ORDER BY Month
LIMIT 10
""")
# line chart


# In[74]:


monthly_sales_by_customer.show(10, truncate=False)
monthly_sales_by_customer.write.mode("overwrite").parquet("outputs/monthly_sales_by_customer")


# In[75]:


top_intl_product_sku_by_amount = spark.sql("""
SELECT SKU, SUM(Gross_Amount) AS Total_Gross_Amount
FROM international_sales
WHERE SKU != 'Unknown'
GROUP BY SKU
ORDER BY Total_Gross_Amount DESC
LIMIT 10
""")
# bar chart


# In[76]:


top_intl_product_sku_by_amount.show(10, truncate=False)
top_intl_product_sku_by_amount.write.mode("overwrite").parquet("outputs/top_intl_product_sku_by_amount")


# In[77]:


pricing_across_platforms = spark.sql("""
SELECT
  Sku,
  `Amazon MRP`,
  `Amazon FBA MRP`,
  `Myntra MRP`,
  `Ajio MRP`,
  `Flipkart MRP`,
  `Snapdeal MRP`,
  `Paytm MRP`,
  `Limeroad MRP`
FROM pricing
ORDER BY Sku DESC
""")


# In[78]:


pricing_across_platforms.show(20, truncate=False)
pricing_across_platforms.write.mode("overwrite").parquet("outputs/pricing_across_platforms")


# In[80]:


max_price_diff_across_platforms = spark.sql("""
SELECT
  SKU,
  `Amazon MRP`,
  `Amazon FBA MRP`,
  `Myntra MRP`,
  `Ajio MRP`,
  `Flipkart MRP`,
  `Snapdeal MRP`,
  `Paytm MRP`,
  `Limeroad MRP`,
  
  GREATEST(
    `Amazon MRP`, `Amazon FBA MRP`, `Myntra MRP`, `Ajio MRP`,
    `Flipkart MRP`, `Snapdeal MRP`, `Paytm MRP`, `Limeroad MRP`
  ) -
  LEAST(
    `Amazon MRP`, `Amazon FBA MRP`, `Myntra MRP`, `Ajio MRP`,
    `Flipkart MRP`, `Snapdeal MRP`, `Paytm MRP`, `Limeroad MRP`
  ) AS Max_Channel_Diff,

  CASE GREATEST(
    `Amazon MRP`, `Amazon FBA MRP`, `Myntra MRP`, `Ajio MRP`,
    `Flipkart MRP`, `Snapdeal MRP`, `Paytm MRP`, `Limeroad MRP`
  )
    WHEN `Amazon MRP` THEN 'Amazon MRP'
    WHEN `Amazon FBA MRP` THEN 'Amazon FBA MRP'
    WHEN `Myntra MRP` THEN 'Myntra MRP'
    WHEN `Ajio MRP` THEN 'Ajio MRP'
    WHEN `Flipkart MRP` THEN 'Flipkart MRP'
    WHEN `Snapdeal MRP` THEN 'Snapdeal MRP'
    WHEN `Paytm MRP` THEN 'Paytm MRP'
    WHEN `Limeroad MRP` THEN 'Limeroad MRP'
  END AS Max_Platform,

  CASE LEAST(
    `Amazon MRP`, `Amazon FBA MRP`, `Myntra MRP`, `Ajio MRP`,
    `Flipkart MRP`, `Snapdeal MRP`, `Paytm MRP`, `Limeroad MRP`
  )
    WHEN `Amazon MRP` THEN 'Amazon MRP'
    WHEN `Amazon FBA MRP` THEN 'Amazon FBA MRP'
    WHEN `Myntra MRP` THEN 'Myntra MRP'
    WHEN `Ajio MRP` THEN 'Ajio MRP'
    WHEN `Flipkart MRP` THEN 'Flipkart MRP'
    WHEN `Snapdeal MRP` THEN 'Snapdeal MRP'
    WHEN `Paytm MRP` THEN 'Paytm MRP'
    WHEN `Limeroad MRP` THEN 'Limeroad MRP'
  END AS Min_Platform

FROM pricing
ORDER BY Max_Channel_Diff DESC
""")


# In[82]:


max_price_diff_across_platforms.show(truncate=False)
max_price_diff_across_platforms.write.mode("overwrite").parquet("outputs/max_price_diff_across_platforms")


# In[ ]:




