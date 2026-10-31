"""Read pipeline artifacts only: no API requests or claims about DAG status."""

import pandas as pd
import plotly.express as px
import streamlit as st

from asteroid_monitor.storage import read_csv, read_json, root_path, validate_outputs

st.set_page_config(page_title="Asteroid Monitor", page_icon="☄", layout="wide")
st.title("Asteroid Monitor")
st.caption("NASA NEAR-EARTH OBJECTS · Observed approach data")
st.info("Potentially hazardous is an orbital classification, not an imminent collision warning.")
root = root_path()
if st.button("Refresh pipeline outputs"):
    st.rerun()
try:
    manifest = read_json(root / "reports/latest.json")
    csv_path = root / manifest["csv_path"]
    report_path = root / manifest["report_path"]
    report = validate_outputs(str(csv_path), str(report_path))
    frame = read_csv(csv_path)
except FileNotFoundError:
    st.info("No published dataset yet. Execute nasa_asteroid_monitor in Airflow, then refresh.")
    st.stop()
except (ValueError, KeyError, TypeError, OSError, pd.errors.ParserError) as exc:
    st.error(f"Pipeline outputs cannot be read: {exc}. Re-run the DAG and refresh.")
    st.stop()

cards = st.columns(4)
cards[0].metric("Total asteroids", report["total_asteroids"])
cards[1].metric("Potentially hazardous", report["hazardous_count"])
closest = report["closest_asteroid"]
fastest = report["fastest_asteroid"]
cards[2].metric("Closest approach", f"{closest['miss_distance_km']:,.0f} km" if closest else "N/A")
cards[3].metric("Highest velocity", f"{fastest['velocity_km_s']:,.2f} km/s" if fastest else "N/A")
st.subheader("Approach overview")
colors = {True: "#B8A5FF", False: "#59CBE8"}
if frame.empty:
    st.info("NASA returned no Earth approaches for this query date.")
else:
    scatter = px.scatter(
        frame,
        x="diameter_km",
        y="velocity_km_s",
        color="is_hazardous",
        hover_name="name",
        color_discrete_map=colors,
        labels={
            "diameter_km": "Diameter midpoint (km)",
            "velocity_km_s": "Approach velocity (km/s)",
            "is_hazardous": "Potentially hazardous",
        },
    )
    st.plotly_chart(scatter, width="stretch")
    left, right = st.columns(2)
    with left:
        st.plotly_chart(
            px.bar(
                frame.nsmallest(10, "miss_distance_km"),
                x="miss_distance_km",
                y="name",
                orientation="h",
                title="10 closest asteroids",
                color_discrete_sequence=["#59CBE8"],
                labels={"miss_distance_km": "Miss distance (km)", "name": "Name"},
            ),
            width="stretch",
        )
    with right:
        counts = pd.DataFrame(
            {
                "Classification": ["Potentially hazardous", "Non-hazardous"],
                "Count": [report["hazardous_count"], len(frame) - report["hazardous_count"]],
            }
        )
        st.plotly_chart(
            px.pie(
                counts,
                names="Classification",
                values="Count",
                hole=0.65,
                title="Hazard classification",
                color_discrete_sequence=["#B8A5FF", "#59CBE8"],
            ),
            width="stretch",
        )

st.subheader("Asteroid explorer")
search = st.text_input("Search asteroid name")
classification = st.selectbox(
    "Hazard classification", ["All", "Potentially hazardous", "Non-hazardous"]
)
sort = st.selectbox("Sort by", ["Closest approach first", "Highest velocity first"])
filtered = frame[frame["name"].str.contains(search, case=False, regex=False, na=False)]
if classification != "All":
    filtered = filtered[filtered["is_hazardous"] == (classification == "Potentially hazardous")]
filtered = filtered.sort_values(
    "velocity_km_s" if sort == "Highest velocity first" else "miss_distance_km",
    ascending=sort != "Highest velocity first",
)
st.dataframe(filtered, hide_index=True, width="stretch")
st.download_button(
    "Download filtered CSV",
    filtered.to_csv(index=False),
    file_name="asteroid_explorer.csv",
    mime="text/csv",
)
st.subheader("Pipeline information")
st.write(f"Dataset generated (UTC): {report['generated_at']}")
st.write(f"Processed records: {len(frame)} · Source: {report['source']}")
st.write(f"Last validated report query date: {report['query_date']}")
st.caption(
    "The manifest identifies the most recently published validated dataset, "
    "including historical runs. It does not confirm Airflow run success. "
    "Check task and run status in the Airflow UI."
)
