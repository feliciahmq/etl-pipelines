import streamlit as st
import pandas as pd
import altair as alt
from pyspark.sql import SparkSession
import plotly.express as px

spark = SparkSession.builder.appName("dashboard").getOrCreate()

def query_to_df(query: str):
    return spark.sql(query).toPandas()

st.set_page_config(layout="wide")
st.title("E-Commerce Sales Dashboard")

tab = st.sidebar.radio("Select View", ["📊 Local Sales", "🌍 International Sales", "🛒 Pricing"])

if tab == "📊 Local Sales":
    st.header("📊 Local Sales Overview")

    st.subheader("Monthly Sales by Category")
    df_monthly_sales = spark.read.parquet("outputs/monthly_sales_by_category")
    df_monthly_sales.createOrReplaceTempView("monthly_sales_by_category")
    df_monthly_sales_pd = query_to_df("SELECT * FROM monthly_sales_by_category")
    bar_data = df_monthly_sales_pd.groupby("Category")["Revenue"].sum().reset_index()
    st.altair_chart(
        alt.Chart(df_monthly_sales_pd).mark_line().encode(
            x='Month:T',
            y='Revenue:Q',
            color='Category:N',
            tooltip=['Month', 'Category', 'Revenue']
        ).interactive(),
        use_container_width=True
    )

    st.subheader("Top SKUs by Amount")
    df_top_sku = spark.read.parquet("outputs/top_product_sku_by_amount")
    df_top_sku.createOrReplaceTempView("top_product_sku_by_amount")
    df_top_sku_pd = query_to_df("SELECT * FROM top_product_sku_by_amount")
    st.dataframe(df_top_sku_pd)

    st.subheader("Sales by Fulfilment Type")
    df_fulfilment = spark.read.parquet("outputs/sales_by_fulfilment")
    df_fulfilment.createOrReplaceTempView("sales_by_fulfilment")
    df_fulfilment_pd = query_to_df("SELECT * FROM sales_by_fulfilment")
    fig = px.pie(df_fulfilment_pd, values='Revenue', names='Fulfilment', title='Sales by Fulfilment')
    st.plotly_chart(fig)

# 🌍 INTERNATIONAL SALES
elif tab == "🌍 International Sales":
    st.header("🌍 International Sales Overview")

    st.subheader("Monthly Sales by Customer")
    df_int_monthly = spark.read.parquet("outputs/monthly_sales_by_customer")
    df_int_monthly.createOrReplaceTempView("monthly_sales_by_customer")
    df_int_monthly_pd = query_to_df("SELECT * FROM monthly_sales_by_customer")
    bar_customer = df_int_monthly_pd.groupby("CUSTOMER")["Revenue"].sum().reset_index()
    st.bar_chart(bar_customer.set_index("CUSTOMER"))

    st.subheader("Top International SKUs by Gross Amount")
    df_top_intl = spark.read.parquet("outputs/top_intl_product_sku_by_amount")
    df_top_intl.createOrReplaceTempView("top_intl_product_sku_by_amount")
    df_top_intl_pd = query_to_df("SELECT * FROM top_intl_product_sku_by_amount")
    st.dataframe(df_top_intl_pd)

# 🛒 PRICING
elif tab == "🛒 Pricing":
    st.header("🛒 Price Comparison Across Channels")

    st.subheader("Pricing Across Platforms")
    df_price = spark.read.parquet("outputs/pricing_across_platforms")
    df_price.createOrReplaceTempView("pricing_across_platforms")
    df_price_pd = query_to_df("SELECT * FROM pricing_across_platforms")
    st.dataframe(df_price_pd)

    st.subheader("Max Price Difference Per SKU")
    df_diff = spark.read.parquet("outputs/max_price_diff_across_platforms")
    df_diff.createOrReplaceTempView("max_price_diff_across_platforms")
    df_diff_pd = query_to_df("SELECT * FROM max_price_diff_across_platforms")
    st.dataframe(df_diff_pd)

    st.subheader("Average Price Per SKU")
    df_avg = spark.read.parquet("outputs/avg_price_per_sku")
    df_avg.createOrReplaceTempView("avg_price_per_sku")
    df_avg_pd = query_to_df("SELECT * FROM avg_price_per_sku")
    st.dataframe(df_avg_pd)
