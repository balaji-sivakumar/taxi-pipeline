import polars as pl
import requests
import streamlit as st

API_URL = "http://127.0.0.1:8000/predictions/next-day"

st.title("Next-Day Pickup Demand")

response = requests.get(API_URL, timeout=10)
response.raise_for_status()
predictions = pl.DataFrame(response.json())

zones = sorted(predictions["pickup_location_id"].unique().to_list())
selected_zone = st.selectbox("Zone", zones)

zone_predictions = (
    predictions.filter(pl.col("pickup_location_id") == selected_zone)
    .sort("pickup_hour")
    .select("pickup_hour", "predicted_pickups")
)

st.line_chart(zone_predictions, x="pickup_hour", y="predicted_pickups")
st.dataframe(zone_predictions)
