import streamlit as st
import pandas as pd
import time
import plotly.express as px

from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import sum, col

session = get_active_session()

st.set_page_config(layout="wide")


# ---- PUT SNOWFLAKE TABLE INTO DF ----

table_name = "COMPANY.GENERAL.SALES"
df = session.table(table_name).to_pandas()
df["hour"] = pd.to_datetime(df["TIME"], format="%H:%M:%S").dt.hour

# ---- SIDEBAR ----

st.sidebar.header("Please Filter Here:")
city = st.sidebar.multiselect(
    "Select the City:",
    options=df["CITY"].unique(),
    default=df["CITY"].unique()
)

customer_type = st.sidebar.multiselect(
    "Select the Customer Type:",
    options=df["CUSTOMER_TYPE"].unique(),
    default=df["CUSTOMER_TYPE"].unique(),
)

gender = st.sidebar.multiselect(
    "Select the Gender:",
    options=df["GENDER"].unique(),
    default=df["GENDER"].unique()
)

df_selection = df.query(
    "CITY == @city & CUSTOMER_TYPE ==@customer_type & GENDER == @gender"
)


# Check if the dataframe is empty:

if df_selection.empty:
    st.warning("No data available based on the current filter settings!")
    st.stop() # This will stop the app for further execution.

# ---- MAINPAGE ----

st.title(":bar_chart: Sales Dashboard")
st.markdown("##")

# TOP KPI's

total_sales = int(df_selection["TOTAL"].sum())
average_rating = round(df_selection["RATING"].mean(), 1)
star_rating = ":star:" * int(round(average_rating, 0))
average_sale_by_transaction = round(df_selection["TOTAL"].mean(), 2)

left_column,middle_column,right_column, = st.columns(3)
with left_column:
    st.subheader("Total Sales:")
    st.subheader(f"US $ {total_sales:,}")
with middle_column:
    st.subheader("Average Rating:")
    st.subheader(f"{average_rating} {star_rating}")
with right_column:
    st.subheader("Average Sales Per Transaction:")
    st.subheader(f"US $ {average_sale_by_transaction}")

st.markdown("""---""")

st.subheader("Sales:")
st.dataframe(df_selection)


st.markdown("""---""")



# SALES BY PRODUCT LINE [BAR CHART]
sales_by_product_line = df_selection.groupby(by=["PRODUCT_LINE"])[["TOTAL"]].sum().sort_values(by="TOTAL")
fig_product_sales = px.bar(
    sales_by_product_line,
    x="TOTAL",
    y=sales_by_product_line.index,
    orientation="h",
    title="<b>Sales by Product Line</b>",
    color_discrete_sequence=["#0083B8"] * len(sales_by_product_line),
    template="plotly_white",
)
fig_product_sales.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    xaxis=(dict(showgrid=False))
)

# SALES BY HOUR [BAR CHART]
sales_by_hour = df_selection.groupby(by=["hour"])[["TOTAL"]].sum()
fig_hourly_sales = px.bar(
    sales_by_hour,
    x=sales_by_hour.index,
    y="TOTAL",
    title="<b>Sales by hour</b>",
    color_discrete_sequence=["#0083B8"] * len(sales_by_hour),
    template="plotly_white",
)
fig_hourly_sales.update_layout(
    xaxis=dict(tickmode="linear"),
    plot_bgcolor="rgba(0,0,0,0)",
    yaxis=(dict(showgrid=False)),
)

left_column,  right_column = st.columns(2)
right_column.plotly_chart(fig_product_sales, use_container_width=True)
left_column.plotly_chart(fig_hourly_sales, use_container_width=True)




# HTML AND CSS IS NOT SUPPORTED INSIDE SNOWFLAKE VERSION

hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)



