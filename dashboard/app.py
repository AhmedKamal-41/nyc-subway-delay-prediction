import streamlit as st
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Tuple

# Configuration
API_URL = "http://localhost:8000"

# Hardcoded tracked stops for MVP
TRACKED_STOPS: List[Tuple[str, str]] = [
    ("A", "A12"),
    ("A", "A24"),
    ("A", "A32"),
    ("1", "101"),
    ("1", "103"),
    ("1", "104"),
    ("2", "201"),
    ("2", "224"),
    ("3", "301"),
    ("4", "401"),
    ("4", "418"),
    ("5", "501"),
    ("6", "601"),
    ("B", "B12"),
    ("B", "B23"),
    ("C", "C12"),
    ("D", "D14"),
    ("E", "E01"),
    ("F", "F12"),
    ("G", "G22"),
    ("J", "J12"),
    ("L", "L01"),
    ("M", "M11"),
    ("N", "N08"),
    ("Q", "Q01"),
    ("R", "R11"),
    ("S", "S01"),
    ("Z", "Z01"),
]


def fetch_predictions(stops: List[Tuple[str, str]], api_url: str) -> List[Dict]:
    """Fetch predictions from API for given stops."""
    predictions = []
    
    for line_id, stop_id in stops:
        try:
            response = requests.post(
                f"{api_url}/predict",
                json={"line_id": line_id, "stop_id": stop_id},
                timeout=5
            )
            
            if response.status_code == 200:
                data = response.json()
                predictions.append({
                    "line_id": data["line_id"],
                    "stop_id": data["stop_id"],
                    "risk_probability": data["risk_probability"],
                    "risk_label": data["risk_label"],
                    "as_of": data["as_of"]
                })
            else:
                st.warning(f"Failed to get prediction for {line_id}/{stop_id}: {response.status_code}")
        
        except requests.exceptions.RequestException as e:
            st.warning(f"Error fetching prediction for {line_id}/{stop_id}: {str(e)}")
    
    return predictions


def main():
    st.set_page_config(page_title="NYC Subway Delay Risk Dashboard", page_icon="🚇")
    
    st.title("🚇 NYC Subway Delay Risk Dashboard")
    st.markdown("Monitor real-time delay risk predictions for tracked subway stations.")
    
    # Sidebar filters
    with st.sidebar:
        st.header("Filters")
        
        line_filter = st.text_input(
            "Filter by Line ID",
            value="",
            help="Enter a line ID to filter results (e.g., 'A', '1', '2'). Leave empty for all lines."
        )
        
        top_n = st.number_input(
            "Number of Results",
            min_value=1,
            max_value=50,
            value=10,
            help="Show top N stations by risk probability"
        )
        
        if st.button("🔄 Refresh", type="primary"):
            st.rerun()
    
    # Check API health
    try:
        health_response = requests.get(f"{API_URL}/health", timeout=2)
        if health_response.status_code != 200:
            st.error(f"API health check failed. Status: {health_response.status_code}")
            st.stop()
    except requests.exceptions.RequestException as e:
        st.error(f"Cannot connect to API at {API_URL}. Please ensure the API is running.")
        st.info("Start the API with: `docker compose up -d`")
        st.stop()
    
    # Filter tracked stops
    filtered_stops = TRACKED_STOPS
    if line_filter:
        filtered_stops = [(line_id, stop_id) for line_id, stop_id in TRACKED_STOPS 
                         if line_id.upper() == line_filter.upper()]
        
        if not filtered_stops:
            st.warning(f"No tracked stops found for line '{line_filter}'")
            st.stop()
    
    # Fetch predictions
    with st.spinner("Fetching predictions..."):
        predictions = fetch_predictions(filtered_stops, API_URL)
    
    if not predictions:
        st.warning("No predictions available. Please check the API connection and try again.")
        st.stop()
    
    # Convert to DataFrame and sort
    df = pd.DataFrame(predictions)
    df = df.sort_values("risk_probability", ascending=False)
    df = df.head(top_n)
    
    # Format columns
    df_display = df.copy()
    df_display["risk_probability"] = df_display["risk_probability"].apply(lambda x: f"{x:.4f}")
    df_display["risk_label"] = df_display["risk_label"].apply(lambda x: "🔴 High Risk" if x == 1 else "🟢 Low Risk")
    
    # Parse and format as_of timestamp
    df_display["as_of"] = pd.to_datetime(df_display["as_of"]).dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    
    # Display results
    st.header(f"Top {len(df_display)} Risky Stations")
    
    if line_filter:
        st.info(f"Showing results for line: **{line_filter.upper()}**")
    
    # Display table
    st.dataframe(
        df_display[["line_id", "stop_id", "risk_probability", "risk_label", "as_of"]],
        use_container_width=True,
        hide_index=True
    )
    
    # Summary statistics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Stations", len(predictions))
    with col2:
        high_risk_count = sum(1 for p in predictions if p["risk_label"] == 1)
        st.metric("High Risk Stations", high_risk_count)
    with col3:
        if predictions:
            avg_risk = sum(p["risk_probability"] for p in predictions) / len(predictions)
            st.metric("Average Risk", f"{avg_risk:.4f}")


if __name__ == "__main__":
    main()

