import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
import os

st.set_page_config(layout="wide")
st.title("E-Commerce Sales Dashboard")

tab = st.sidebar.radio("Select View", ["📊 Local Sales", "🌍 International Sales", "🛒 Pricing"])

# Load all data using Pandas (from Parquet files)
@st.cache_data
def load_data(path):
    return pd.read_parquet(path)

if tab == "📊 Local Sales":
    st.header("📊 Local Sales Overview")

    st.subheader("Monthly Sales by Top 5 Category")
    df_monthly_sales = load_data("part2/outputs/monthly_sales_by_category")
    st.altair_chart(
        alt.Chart(df_monthly_sales).mark_line().encode(
            x='Month:T',
            y='Revenue:Q',
            color='Category:N',
            tooltip=['Month', 'Category', 'Revenue']
        ).interactive(),
        use_container_width=True
    )

    st.subheader("Top 10 SKUs by Amount")
    df_top_sku = load_data("part2/outputs/top_product_sku_by_amount")
    st.dataframe(df_top_sku)

    st.subheader("Sales by Fulfilment Type")
    df_fulfilment = load_data("part2/outputs/sales_by_fulfilment")
    fig = px.pie(df_fulfilment, values='Revenue', names='Fulfilment', title='Sales by Fulfilment')
    st.plotly_chart(fig)

elif tab == "🌍 International Sales":
    st.header("🌍 International Sales Overview")

    st.subheader("Monthly Sales by Top 10 Customer")
    df_int_monthly = load_data("part2/outputs/monthly_sales_by_customer")
    customer_total = df_int_monthly.groupby("CUSTOMER")["Revenue"].sum().reset_index()
    st.bar_chart(customer_total.set_index("CUSTOMER"))

    st.subheader("Top 10 International SKUs by Gross Amount")
    df_top_intl = load_data("part2/outputs/top_intl_product_sku_by_amount")
    st.dataframe(df_top_intl)

elif tab == "🛒 Pricing":
    st.header("🛒 Price Comparison Across Platforms")

    st.subheader("Pricing Across Platforms")
    df_price = load_data("part2/outputs/pricing_across_platforms")
    
    sku_query = st.text_input("Search by SKU (case sensitive)")
    if sku_query:
        df_price = df_price[df_price["Sku"].str.contains(sku_query, case=False, na=False)]

    st.dataframe(df_price)

    st.subheader("Max Price Difference Across Platforms")
    df_diff = load_data("part2/outputs/max_price_diff_across_platforms")
    selected_columns = ["SKU", "Max_Channel_Diff", "Max_Platform", "Min_Platform"]
    sku_query = st.text_input("Search by SKU (case insensitive)")
    if sku_query:
        df_filtered = df_diff[df_diff["SKU"].str.contains(sku_query, case=False, na=False)]
    else:
        df_filtered = df_diff

    st.dataframe(df_filtered[selected_columns])
