"""
map_engine.py
─────────────────────────────────────────────────────────────────────────────
Standalone pydeck map builder for NYC restaurant inspection data.

No Streamlit imports at the top level — safe to import from any runtime
(Streamlit in Snowflake, Jupyter, plain Python scripts).

Expected DataFrame columns
--------------------------
Required : LATITUDE, LONGITUDE, DBA, GRADE, SCORE, BOROUGH
Optional : CUISINE  (shown in tooltip; defaults to empty string if absent)

Both pandas.DataFrame and snowflake.snowpark.DataFrame are accepted;
Snowpark frames are converted via .to_pandas() before rendering.

Public API
----------
    build_deck(df, layer_mode='scatter') -> pydeck.Deck
    GRADE_COLORS : dict[str, list[int]]   importable colour palette

Colour scheme
-------------
    A → green  [0, 180, 0]
    B → amber  [255, 165, 0]
    C → red    [220, 50, 50]
    Z / P / G / N / null → grey [150, 150, 150]

Designed for reuse in:
    map/streamlit_map_app.py          — standalone SiS explorer (Phase 4)
    memory/streamlit_app.py           — Phase 5 agent Streamlit app
"""

from __future__ import annotations

import pandas as pd
import pydeck as pdk

# ── Colour palette — importable by consumers ──────────────────────────────────

GRADE_COLORS: dict[str, list[int]] = {
    "A": [0,   180,   0, 210],   # green
    "B": [255, 165,   0, 210],   # amber
    "C": [220,  50,  50, 210],   # red
}
_DEFAULT_COLOR: list[int] = [150, 150, 150, 180]   # Z, P, G, N, null → grey

# ── NYC default viewport ──────────────────────────────────────────────────────

NYC_VIEW = pdk.ViewState(
    latitude=40.7128,
    longitude=-74.0060,
    zoom=10,
    pitch=0,
    bearing=0,
)

# ── Tooltip (scatter mode only) ───────────────────────────────────────────────

_TOOLTIP = {
    "html": (
        "<b>{DBA}</b><br/>"
        "{STREET_ADDRESS}<br/>"
        "Grade: <b>{GRADE}</b>&nbsp;&nbsp;Score: {SCORE}<br/>"
        "Borough: {BOROUGH}<br/>"
        "Cuisine: {CUISINE}<br/>"
        "Last inspection: {LAST_INSPECTION}"
    ),
    "style": {
        "backgroundColor": "rgba(0,0,0,0.78)",
        "color": "white",
        "fontSize": "12px",
        "padding": "8px 10px",
        "borderRadius": "4px",
        "maxWidth": "240px",
    },
}


# ── Internal helpers ──────────────────────────────────────────────────────────

def _to_pandas(df) -> pd.DataFrame:
    """Accept pandas or Snowpark DataFrames transparently."""
    if hasattr(df, "to_pandas"):          # snowflake.snowpark.DataFrame
        return df.to_pandas()
    return df.copy()


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce types, drop null coordinates, add FILL_COLOR."""
    df = df.copy()

    # TRY_TO_DECIMAL arrives as Python Decimal — coerce to float
    df["LATITUDE"]  = pd.to_numeric(df["LATITUDE"],  errors="coerce")
    df["LONGITUDE"] = pd.to_numeric(df["LONGITUDE"], errors="coerce")
    df = df.dropna(subset=["LATITUDE", "LONGITUDE"])

    # Normalise grade to uppercase string; treat NaN / None as ""
    df["GRADE"] = df["GRADE"].fillna("").astype(str).str.upper()

    # CUISINE is optional — default to empty string for tooltip
    if "CUISINE" not in df.columns:
        df["CUISINE"] = ""
    df["CUISINE"] = df["CUISINE"].fillna("")

    # STREET_ADDRESS is optional
    if "STREET_ADDRESS" not in df.columns:
        df["STREET_ADDRESS"] = ""
    df["STREET_ADDRESS"] = df["STREET_ADDRESS"].fillna("")

    # LAST_INSPECTION is optional — shown in tooltip when present
    if "LAST_INSPECTION" not in df.columns:
        df["LAST_INSPECTION"] = ""
    df["LAST_INSPECTION"] = df["LAST_INSPECTION"].astype(str).replace("NaT", "").replace("None", "")

    # SCORE may be None/NaN — display as "–"
    df["SCORE"] = df["SCORE"].apply(
        lambda v: "–" if pd.isna(v) else str(int(v)) if isinstance(v, float) else str(v)
    )

    # Add colour list column
    df["FILL_COLOR"] = df["GRADE"].map(
        lambda g: GRADE_COLORS.get(g, _DEFAULT_COLOR)
    )

    return df


# ── Layer builders ────────────────────────────────────────────────────────────

def _scatter_layer(df: pd.DataFrame) -> pdk.Layer:
    return pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["LONGITUDE", "LATITUDE"],
        get_fill_color="FILL_COLOR",
        get_line_color=[0, 0, 0, 60],
        get_radius=40,                  # metres — visible at zoom 10–14
        radius_min_pixels=3,
        radius_max_pixels=14,
        pickable=True,
        stroked=True,
        line_width_min_pixels=1,
    )


def _heatmap_layer(df: pd.DataFrame) -> pdk.Layer:
    return pdk.Layer(
        "HeatmapLayer",
        data=df,
        get_position=["LONGITUDE", "LATITUDE"],
        get_weight=1,
        radius_pixels=40,
        intensity=1,
        threshold=0.05,
        pickable=False,                 # HeatmapLayer does not support tooltips
    )


# ── Public API ────────────────────────────────────────────────────────────────

def build_deck(df, layer_mode: str = "scatter") -> pdk.Deck:
    """
    Build a pydeck.Deck from NYC inspection data.

    Parameters
    ----------
    df : pandas.DataFrame | snowflake.snowpark.DataFrame
        Required columns: LATITUDE, LONGITUDE, DBA, GRADE, SCORE, BOROUGH
        Optional column : CUISINE (shown in tooltip)
    layer_mode : {"scatter", "heatmap"}
        "scatter"  — colour-coded points, one per restaurant (default).
        "heatmap"  — density heatmap; grade colours are not applied.

    Returns
    -------
    pydeck.Deck
        Pass directly to st.pydeck_chart().
    """
    pdf = _to_pandas(df)
    pdf = _prepare(pdf)

    if layer_mode == "heatmap":
        layer   = _heatmap_layer(pdf)
        tooltip = False
    else:
        layer   = _scatter_layer(pdf)
        tooltip = _TOOLTIP

    return pdk.Deck(
        layers=[layer],
        initial_view_state=NYC_VIEW,
        tooltip=tooltip,
        # Light basemap — Streamlit injects a Mapbox token automatically
        # when rendering via st.pydeck_chart, so no user token is required.
        map_style="mapbox://styles/mapbox/light-v10",
    )
