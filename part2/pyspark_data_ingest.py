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


# In[19]:


from pyspark.sql.functions import date_format

df_sales = df_sales.withColumn("Month", date_format("Date", "yyyy-MM"))


# In[20]:


df_sales.createOrReplaceTempView("sales")
df_international.createOrReplaceTempView("international_sales")
df_pricing.createOrReplaceTempView("pricing")


# In[21]:


print(df_sales.columns)


# In[22]:


print(df_international.columns)


# In[23]:


print(df_pricing.columns)


# In[24]:


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


# In[25]:


monthly_sales_by_category.show(10, truncate=False)
monthly_sales_by_category.write.mode("overwrite").parquet("outputs/monthly_sales_by_category")


# In[29]:


top_product_sku_by_amount = spark.sql("""
SELECT SKU, SUM(Amount) AS Total_Gross, COUNT(*) AS Orders
FROM sales
GROUP BY SKU
ORDER BY Total_Gross DESC
LIMIT 10    
""")
# bar chart


# In[30]:


top_product_sku_by_amount.show(10, truncate=False)
top_product_sku_by_amount.write.mode("overwrite").parquet("outputs/top_product_sku_by_amount")


# In[31]:


sales_by_fulfilment = spark.sql("""
SELECT Fulfilment, SUM(Amount) AS Revenue, COUNT(*) AS Orders
FROM sales
GROUP BY Fulfilment
""")
# pie chart


# In[32]:


sales_by_fulfilment.show(10, truncate=False)
sales_by_fulfilment.write.mode("overwrite").parquet("outputs/sales_by_fulfilment")


# In[33]:


monthly_sales_by_customer = spark.sql("""
SELECT Month, CUSTOMER, SUM(Gross_Amount) AS Revenue
FROM international_sales
GROUP BY Month, CUSTOMER
ORDER BY Month
LIMIT 10
""")
# line chart


# In[34]:


monthly_sales_by_customer.show(10, truncate=False)
monthly_sales_by_customer.write.mode("overwrite").parquet("outputs/monthly_sales_by_customer")


# In[40]:


top_intl_product_sku_by_amount = spark.sql("""
SELECT SKU, SUM(Gross_Amount) AS Total_Gross_Amount
FROM international_sales
WHERE SKU != 'Unknown'
GROUP BY SKU
ORDER BY Total_Gross_Amount DESC
LIMIT 10
""")
# bar chart


# In[41]:


top_intl_product_sku_by_amount.show(10, truncate=False)
top_intl_product_sku_by_amount.write.mode("overwrite").parquet("outputs/top_intl_product_sku_by_amount")


# In[42]:


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


# In[43]:


pricing_across_platforms.show(20, truncate=False)
pricing_across_platforms.write.mode("overwrite").parquet("outputs/pricing_across_platforms")


# In[44]:


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
  ) AS Max_Channel_Diff
FROM pricing
ORDER BY Max_Channel_Diff DESC
""")


# In[45]:


max_price_diff_across_platforms.show(truncate=False)
max_price_diff_across_platforms.write.mode("overwrite").parquet("outputs/max_price_diff_across_platforms")


# In[113]:


avg_price_per_sku = spark.sql("""
SELECT
  Sku,
  AVG(`Amazon MRP`) AS Avg_Amazon,
  AVG(`Amazon FBA MRP`) AS Avg_FBA,
  AVG(`Myntra MRP`) AS Avg_Myntra,
  AVG(`Ajio MRP`) AS Avg_Ajio,
  AVG(`Flipkart MRP`) AS Avg_Flipkart,
  AVG(`Snapdeal MRP`) AS Avg_Snapdeal,
  AVG(`Paytm MRP`) AS Avg_Paytm,
  AVG(`Limeroad MRP`) AS Avg_Limeroad
FROM pricing
GROUP BY Sku
HAVING
  AVG(`Amazon MRP`) IS NOT NULL OR
  AVG(`Amazon FBA MRP`) IS NOT NULL OR
  AVG(`Myntra MRP`) IS NOT NULL OR
  AVG(`Ajio MRP`) IS NOT NULL OR
  AVG(`Flipkart MRP`) IS NOT NULL OR
  AVG(`Snapdeal MRP`) IS NOT NULL OR
  AVG(`Paytm MRP`) IS NOT NULL OR
  AVG(`Limeroad MRP`) IS NOT NULL
ORDER BY Sku
""")


# In[114]:


avg_price_per_sku.show(truncate=False)
avg_price_per_sku.write.mode("overwrite").parquet("outputs/avg_price_per_sku")


# In[ ]:





# In[ ]:




