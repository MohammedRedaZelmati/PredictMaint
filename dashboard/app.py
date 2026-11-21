"""SQL-only live view of SmartPredict test replay predictions."""

import pandas as pd
import psycopg2
import streamlit as st

from data_access import (
    engine_trajectory,
    latest_by_engine,
    latest_kpis,
    list_test_runs,
    model_registry,
)


st.set_page_config(page_title="SmartPredict", page_icon="⚙️", layout="wide")
st.title("SmartPredict")
st.caption("RUL estimé en cycles · données PostgreSQL · actualisation toutes les 5 secondes")


@st.fragment(run_every="5s")
def live_dashboard():
    try:
        runs = list_test_runs()
    except (psycopg2.Error, KeyError) as error:
        st.error(f"Base de données indisponible : {error}")
        return
    if not runs:
        st.info("En attente d'un replay test et de prédictions SQL.")
        return

    run_ids = [row["run_id"] for row in runs]
    chosen_run = st.selectbox("Session test", run_ids, format_func=str)
    latest = latest_by_engine(chosen_run)
    if not latest:
        st.info("En attente de prédictions pour cette session.")
        return

    frame = pd.DataFrame(latest)
    critical = frame[frame["health_status"] == "CRITICAL"]
    warning = frame[frame["health_status"] == "WARNING"]
    latency = pd.to_numeric(frame["latency_seconds"], errors="coerce").median()
    first, second, third, fourth = st.columns(4)
    first.metric("Moteurs suivis", len(frame))
    second.metric("Critiques", len(critical))
    third.metric("À surveiller", len(warning))
    fourth.metric("Latence indicative médiane", f"{latency:.1f} s" if pd.notna(latency) else "—")

    model_version = str(frame.sort_values("predicted_at").iloc[-1]["model_version"])
    registry = model_registry(model_version)
    if registry:
        entry = registry[0]
        st.caption(f"Modèle {model_version} · {entry['algorithm']} · dernière prédiction {frame['predicted_at'].max()}")
    else:
        st.caption(f"Modèle {model_version} · dernière prédiction {frame['predicted_at'].max()}")

    engine = st.selectbox("Moteur", sorted(frame["engine_id"].tolist()))
    trajectory = pd.DataFrame(engine_trajectory(chosen_run, engine))
    if not trajectory.empty:
        st.subheader("RUL estimé par cycle")
        st.line_chart(trajectory.set_index("cycle")["predicted_rul_cycles"], y_label="Cycles restants")

    st.subheader("État actuel des moteurs")
    st.dataframe(
        frame[["engine_id", "cycle", "predicted_rul_cycles", "health_status",
               "model_version", "predicted_at"]].sort_values("predicted_rul_cycles"),
        hide_index=True,
        use_container_width=True,
    )

    st.subheader("Indicateurs batch")
    kpis = latest_kpis()
    if kpis:
        st.dataframe(pd.DataFrame(kpis), hide_index=True, use_container_width=True)
    else:
        st.info("En attente des KPI batch.")


live_dashboard()
