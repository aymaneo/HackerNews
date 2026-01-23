"""
HackerNews Real-Time Dashboard
Visualisation des données Gold Layer (Sentiment, NER, Keywords)
"""

import streamlit as st
import pandas as pd
import time
import os
from deltalake import DeltaTable

# Configuration
GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://garage:3900")
GARAGE_ACCESS_KEY = os.getenv("GARAGE_ACCESS_KEY", "GK2ae23cad2bbbf648143b1b8c")
GARAGE_SECRET_KEY = os.getenv("GARAGE_SECRET_KEY", "997e31832cbc9c78a2d919897f1cc9d63ad2c628464a7fba3a55f972c31790ee")
GOLD_PATH = os.getenv("GOLD_PATH", "s3://gold/hackernews")
REFRESH_INTERVAL = int(os.getenv("REFRESH_INTERVAL", "30"))

# S3 storage options for deltalake
STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": GARAGE_ENDPOINT,
    "AWS_ACCESS_KEY_ID": GARAGE_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": GARAGE_SECRET_KEY,
    "AWS_REGION": "garage",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true",
}

st.set_page_config(
    page_title="HackerNews Analytics",
    page_icon="📊",
    layout="wide"
)


def load_delta_table(table_name: str) -> pd.DataFrame:
    """Load a Delta table and convert to Pandas."""
    try:
        dt = DeltaTable(f"{GOLD_PATH}/{table_name}", storage_options=STORAGE_OPTIONS)
        return dt.to_pandas()
    except Exception as e:
        st.warning(f"Table '{table_name}' non disponible: {e}")
        return pd.DataFrame()


def main():
    st.title("HackerNews Analytics Dashboard")
    st.caption(f"Rafraichissement automatique toutes les {REFRESH_INTERVAL}s")

    # Tabs pour organiser les visualisations
    tab1, tab2, tab3, tab4 = st.tabs([
        "Sentiment Temps Réel",
        "Sentiment par Domaine",
        "Entités (NER)",
        "Mots-clés"
    ])

    # === Tab 1: Sentiment en temps réel ===
    with tab1:
        st.header("Sentiment des commentaires (streaming)")

        sentiment_rt = load_delta_table("sentiment_real_time")

        if not sentiment_rt.empty:
            col1, col2, col3 = st.columns(3)

            total = sentiment_rt["total_comments"].sum()
            positive = sentiment_rt["positive_count"].sum()
            negative = sentiment_rt["negative_count"].sum()

            col1.metric("Total Commentaires", f"{total:,}")
            col2.metric("Positifs", f"{positive:,}", delta=f"{positive/total*100:.1f}%" if total > 0 else "0%")
            col3.metric("Négatifs", f"{negative:,}", delta=f"-{negative/total*100:.1f}%" if total > 0 else "0%")

            # Graphique temporel
            if "window" in sentiment_rt.columns:
                st.subheader("Evolution du ratio positif par fenêtre")
                chart_data = sentiment_rt.copy()
                if isinstance(chart_data["window"].iloc[0], dict):
                    chart_data["window_start"] = chart_data["window"].apply(lambda x: x.get("start", ""))
                    chart_data = chart_data.sort_values("window_start")
                st.line_chart(chart_data.set_index("window_start" if "window_start" in chart_data.columns else chart_data.index)["positive_ratio"])
        else:
            st.info("En attente des données streaming... Exécutez le notebook Gold pour démarrer le streaming.")

    # === Tab 2: Sentiment par domaine ===
    with tab2:
        st.header("Sentiment par domaine source")

        sentiment_domain = load_delta_table("sentiment_by_domain")

        if not sentiment_domain.empty:
            col1, col2 = st.columns([2, 1])

            with col1:
                st.subheader("Top domaines par volume")
                chart_data = sentiment_domain.head(15)
                st.bar_chart(chart_data.set_index("domain")[["positive", "negative"]])

            with col2:
                st.subheader("Ratio positivité")
                st.dataframe(
                    sentiment_domain[["domain", "comment_count", "positive_pct"]]
                    .sort_values("positive_pct", ascending=False)
                    .head(10),
                    hide_index=True
                )
        else:
            st.info("Données non disponibles. Exécutez le notebook Gold.")

    # === Tab 3: Entités NER ===
    with tab3:
        st.header("Entités nommées extraites (NER)")

        entities = load_delta_table("entities")

        if not entities.empty:
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Top entités")
                top_entities = entities.head(20)
                st.bar_chart(top_entities.set_index("entity_text")["count"])

            with col2:
                st.subheader("Par type d'entité")
                if "entity_type" in entities.columns:
                    type_counts = entities.groupby("entity_type")["count"].sum().reset_index()
                    st.bar_chart(type_counts.set_index("entity_type"))

                st.subheader("Détail")
                st.dataframe(entities.head(30), hide_index=True)
        else:
            st.info("Données NER non disponibles. Exécutez le notebook Gold.")

    # === Tab 4: Mots-clés ===
    with tab4:
        st.header("Mots-clés fréquents")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Batch (Top 50)")
            keywords = load_delta_table("keywords")
            if not keywords.empty:
                st.bar_chart(keywords.head(25).set_index("keyword")["count"])
            else:
                st.info("Données non disponibles.")

        with col2:
            st.subheader("Temps réel (fenêtre glissante)")
            keywords_rt = load_delta_table("keywords_real_time")
            if not keywords_rt.empty:
                st.dataframe(keywords_rt.head(20), hide_index=True)
            else:
                st.info("En attente du streaming...")

    # Refresh automatique
    st.divider()

    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("Rafraichir maintenant"):
            st.rerun()
    with col2:
        st.caption(f"Prochain rafraichissement dans {REFRESH_INTERVAL}s...")

    time.sleep(REFRESH_INTERVAL)
    st.rerun()


if __name__ == "__main__":
    main()
