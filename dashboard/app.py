import sys
from pathlib import Path

import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.analytics import format_lap_time, overview_metrics, season_metrics
from dashboard.charts import (
    COMPOUND_COLOURS,
    fastest_lap_chart,
    lap_delta_chart,
    season_driver_chart,
    season_races_chart,
    season_winners_chart,
    telemetry_chart,
    tire_degradation_chart,
    v_min_track_chart,
)
from dashboard.data import DashboardRepository

st.set_page_config(
    page_title="ApexFlow · Race Intelligence",
    page_icon="🏁",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    :root { --apex-red: #ff3154; --panel: #11131a; --muted: #9399aa; }
    .stApp {
        background:
          radial-gradient(circle at 78% -10%, rgba(255,49,84,.13), transparent 34rem),
          linear-gradient(180deg, #090a0f 0%, #0d0f15 100%);
    }
    [data-testid="stSidebar"] { background: #0b0d12; border-right: 1px solid #222631; }
    [data-testid="stMetric"] {
        background: linear-gradient(145deg, rgba(25,28,38,.94), rgba(14,16,23,.94));
        border: 1px solid #292d39; border-radius: 14px; padding: 18px 20px;
        box-shadow: 0 12px 35px rgba(0,0,0,.18);
    }
    [data-testid="stMetricLabel"] { color: var(--muted); text-transform: uppercase; letter-spacing: .08em; }
    [data-testid="stMetricValue"] { color: #f6f7fb; font-variant-numeric: tabular-nums; }
    .hero { padding: 1.2rem 0 .7rem; }
    .eyebrow { color: var(--apex-red); font-size: .78rem; font-weight: 800; letter-spacing: .18em; text-transform: uppercase; }
    .hero h1 { font-size: clamp(2.5rem, 5vw, 4.7rem); line-height: .94; margin: .45rem 0 .8rem; letter-spacing: -.055em; }
    .hero p { color: #a6abba; max-width: 760px; font-size: 1.02rem; }
    .session-chip { display: inline-flex; gap: .55rem; align-items: center; color: #d9dde7; background: #171a23;
        border: 1px solid #2a2e3a; border-radius: 999px; padding: .45rem .8rem; margin-top: .35rem; }
    .session-chip span { color: var(--apex-red); }
    .section-note { color: #8d93a3; margin-top: -.65rem; margin-bottom: 1rem; }
    div[data-testid="stPlotlyChart"] { border: 1px solid #242834; border-radius: 14px; overflow: hidden; background: rgba(14,16,23,.72); }
    .compound-row { display:flex; gap:.9rem; flex-wrap:wrap; color:#aab0bf; font-size:.82rem; }
    .compound-dot { width:.62rem; height:.62rem; border-radius:50%; display:inline-block; margin-right:.32rem; }
    #MainMenu, footer { visibility: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def repository() -> DashboardRepository:
    return DashboardRepository()


@st.cache_data(ttl=900, show_spinner=False)
def load_sessions() -> pd.DataFrame:
    return repository().sessions()

@st.cache_data(ttl=900, show_spinner=False)
def load_season_races(season: int) -> pd.DataFrame:
    return repository().season_races(season)


@st.cache_data(ttl=900, show_spinner=False)
def load_season_drivers(season: int) -> pd.DataFrame:
    return repository().season_drivers(season)


@st.cache_data(ttl=900, show_spinner=False)
def load_season_winners(season: int) -> pd.DataFrame:
    return repository().season_winners(season)


@st.cache_data(ttl=900, show_spinner=False)
def load_race_results(session_key: int) -> pd.DataFrame:
    return repository().race_results(session_key)


@st.cache_data(ttl=900, show_spinner=False)
def load_drivers(session_key: int) -> pd.DataFrame:
    return repository().drivers(session_key)


@st.cache_data(ttl=900, show_spinner=False)
def load_laps(session_key: int, driver_numbers: tuple[int, ...]) -> pd.DataFrame:
    return repository().laps(session_key, driver_numbers)


@st.cache_data(ttl=900, show_spinner=False)
def load_tires(session_key: int, driver_numbers: tuple[int, ...]) -> pd.DataFrame:
    return repository().tire_degradation(session_key, driver_numbers)


@st.cache_data(ttl=900, show_spinner=False)
def load_v_min(session_key: int, driver_numbers: tuple[int, ...]) -> pd.DataFrame:
    return repository().v_min_points(session_key, driver_numbers)


@st.cache_data(ttl=900, show_spinner=False)
def load_telemetry(session_key: int, driver_number: int, lap_number: int) -> pd.DataFrame:
    return repository().telemetry(session_key, driver_number, lap_number)


def stop_with_error(message: str) -> None:
    st.error(message)
    st.info("Check Google Application Default Credentials and the configured BigQuery project.")
    st.stop()


try:
    sessions = load_sessions()
except Exception as exc:
    stop_with_error(f"Could not load dashboard sessions: {exc}")

if sessions.empty:
    stop_with_error("No analytics-ready sessions were found.")

session_labels = {
    int(row.session_key): f"{row.meeting_name} · {row.session_name} ({row.season})"
    for row in sessions.itertuples()
}

season_options = sorted((int(value) for value in sessions["season"].unique()), reverse=True)

with st.sidebar:
    st.markdown("### APEXFLOW")
    st.caption("Race intelligence console")
    dashboard_view = st.radio(
        "Dashboard view",
        options=["Season overview", "Race detail"],
        horizontal=True,
    )

if dashboard_view == "Season overview":
    with st.sidebar:
        selected_season = st.selectbox("Season", options=season_options)
        st.divider()
        st.caption("Data source")
        st.markdown("**BigQuery · Gold layer**")
        st.caption("Only ingested races are included")

    try:
        season_races = load_season_races(selected_season)
        season_drivers = load_season_drivers(selected_season)
        season_winners = load_season_winners(selected_season)
    except Exception as exc:
        stop_with_error(f"Could not load season data: {exc}")

    st.markdown(
        f"""
        <div class="hero">
          <div class="eyebrow">Season intelligence</div>
          <h1>{selected_season} Season</h1>
          <p>A year-level view of every race currently processed through the ApexFlow telemetry lakehouse.</p>
          <div class="session-chip"><span>●</span>{len(season_races)} races available in the Gold layer</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if season_races.empty:
        st.warning("No race summaries are available for this season.")
        st.stop()

    annual_metrics = season_metrics(season_races, season_drivers)
    season_columns = st.columns(4)
    season_columns[0].metric("Races analysed", annual_metrics["race_count"])
    season_columns[1].metric("Drivers", annual_metrics["driver_count"])
    season_columns[2].metric("Laps analysed", annual_metrics["lap_count"])
    season_columns[3].metric(
        "Season fastest lap",
        annual_metrics["fastest_lap"],
        annual_metrics["fastest_driver"],
    )

    st.subheader("Season at a glance")
    st.markdown(
        '<p class="section-note">Race summaries expand automatically as new sessions are ingested and transformed.</p>',
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        season_races_chart(season_races),
        width="stretch",
        config={"displayModeBar": False},
    )
    st.plotly_chart(
        season_driver_chart(season_drivers),
        width="stretch",
        config={"displayModeBar": False},
    )

    st.subheader("Winning drivers")
    st.markdown(
        '<p class="section-note">Official race winners and podium totals from the ingested classification data.</p>',
        unsafe_allow_html=True,
    )
    if season_winners.empty:
        st.info("No official race classifications are available for this season yet.")
    else:
        st.plotly_chart(
            season_winners_chart(season_winners),
            width="stretch",
            config={"displayModeBar": False},
        )
        st.dataframe(
            season_winners[["full_name", "team_name", "wins", "podiums", "points"]].rename(columns={
                "full_name": "Driver",
                "team_name": "Team",
                "wins": "Wins",
                "podiums": "Podiums",
                "points": "Points",
            }),
            hide_index=True,
            width="stretch",
        )

    race_table = season_races.copy()
    race_table["Fastest lap"] = race_table["fastest_lap"].map(format_lap_time)
    race_table["Conditions"] = race_table["was_wet"].map({True: "Wet", False: "Dry"}).fillna("Unknown")
    st.dataframe(
        race_table[[
            "meeting_name", "circuit_short_name", "fastest_driver", "Fastest lap",
            "top_speed", "driver_count", "lap_count", "Conditions",
        ]].rename(columns={
            "meeting_name": "Race",
            "circuit_short_name": "Circuit",
            "fastest_driver": "Fastest driver",
            "top_speed": "Top speed",
            "driver_count": "Drivers",
            "lap_count": "Laps",
        }),
        hide_index=True,
        width="stretch",
    )
    st.stop()

with st.sidebar:
    selected_session = st.selectbox(
        "Race session",
        options=list(session_labels),
        format_func=session_labels.get,
    )

    try:
        drivers = load_drivers(selected_session)
        race_results = load_race_results(selected_session)
    except Exception as exc:
        stop_with_error(f"Could not load race drivers and results: {exc}")

    driver_labels = {
        int(row.driver_number): f"{row.name_acronym} · {row.team_name}"
        for row in drivers.itertuples()
    }
    podium_numbers = (
        race_results.sort_values("finishing_position").head(3)["driver_number"].tolist()
        if not race_results.empty else []
    )
    default_drivers = [int(number) for number in podium_numbers if int(number) in driver_labels]
    if not default_drivers:
        default_drivers = list(driver_labels)[: min(3, len(driver_labels))]
    selected_drivers = st.multiselect(
        "Drivers",
        options=list(driver_labels),
        default=default_drivers,
        format_func=driver_labels.get,
        max_selections=6,
        help="Select up to six drivers to keep comparisons readable.",
    )
    st.divider()
    st.caption("Data source")
    st.markdown("**BigQuery · Gold layer**")
    st.caption("Cached for 15 minutes")

if not selected_drivers:
    st.warning("Select at least one driver to begin the comparison.")
    st.stop()

driver_numbers = tuple(int(number) for number in selected_drivers)
try:
    laps = load_laps(selected_session, driver_numbers)
except Exception as exc:
    stop_with_error(f"Could not load lap data: {exc}")

session = sessions.loc[sessions["session_key"] == selected_session].iloc[0]
st.markdown(
    f"""
    <div class="hero">
      <div class="eyebrow">Race intelligence / {session['season']}</div>
      <h1>{session['meeting_name']}</h1>
      <p>Compare race pace, tire life and minimum-speed signatures across the field.</p>
      <div class="session-chip"><span>●</span>{session['circuit_short_name']} · {session['location']}, {session['country_name']}</div>
    </div>
    """,
    unsafe_allow_html=True,
)

if race_results.empty:
    st.info("Official race classification is not available for this session yet.")
else:
    podium = race_results.sort_values("finishing_position").head(3)
    podium_columns = st.columns(3)
    podium_labels = ["Winner", "Second place", "Third place"]
    for index, row in enumerate(podium.itertuples()):
        detail = row.team_name
        if pd.notna(row.points):
            detail = f"{detail} · {row.points:g} pts"
        podium_columns[index].metric(
            f"P{int(row.finishing_position)} · {podium_labels[index]}",
            row.full_name,
            detail,
        )

if laps.empty:
    st.warning("No lap data exists for the selected drivers.")
    st.stop()

metrics = overview_metrics(laps)
metric_columns = st.columns(4)
metric_columns[0].metric("Fastest lap", metrics["fastest_lap"], metrics["fastest_driver"])
metric_columns[1].metric("Top speed", metrics["top_speed"])
metric_columns[2].metric("Laps analysed", metrics["lap_count"])
metric_columns[3].metric("Drivers", str(laps["driver_number"].nunique()))

overview_tab, laps_tab, tires_tab, track_tab = st.tabs(
    ["Overview", "Lap analysis", "Tire strategy", "V-min & telemetry"]
)

with overview_tab:
    st.subheader("Race pace snapshot")
    if not race_results.empty:
        st.markdown("#### Official classification")
        st.dataframe(
            race_results[["finishing_position", "full_name", "team_name", "points", "gap_to_leader", "result_status"]]
            .rename(columns={
                "finishing_position": "Position",
                "full_name": "Driver",
                "team_name": "Team",
                "points": "Points",
                "gap_to_leader": "Gap",
                "result_status": "Status",
            }),
            hide_index=True,
            width="stretch",
        )

    st.markdown("#### Selected-driver pace")
    st.markdown('<p class="section-note">Each driver’s quickest valid lap in the selected session.</p>', unsafe_allow_html=True)
    st.plotly_chart(fastest_lap_chart(laps), width="stretch", config={"displayModeBar": False})

    leaderboard = (
        laps.sort_values("lap_duration")
        .drop_duplicates("driver_number")
        [["name_acronym", "team_name", "lap_number", "lap_duration", "top_speed", "tire_compound"]]
        .rename(columns={
            "name_acronym": "Driver",
            "team_name": "Team",
            "lap_number": "Lap",
            "lap_duration": "Time (s)",
            "top_speed": "Top speed",
            "tire_compound": "Compound",
        })
    )
    st.dataframe(leaderboard, hide_index=True, width="stretch")

with laps_tab:
    st.subheader("Lap-by-lap performance")
    st.markdown('<p class="section-note">Delta to each driver’s own fastest lap isolates pace evolution.</p>', unsafe_allow_html=True)
    st.plotly_chart(lap_delta_chart(laps), width="stretch", config={"displayModeBar": False})

    clean_laps = laps.loc[~laps["is_pit_out_lap"]].copy()
    clean_laps["Formatted time"] = clean_laps["lap_duration"].map(format_lap_time)
    st.dataframe(
        clean_laps[["name_acronym", "lap_number", "Formatted time", "delta_to_driver_best", "tire_compound", "tire_age_at_lap_end"]]
        .rename(columns={
            "name_acronym": "Driver",
            "lap_number": "Lap",
            "delta_to_driver_best": "Delta (s)",
            "tire_compound": "Compound",
            "tire_age_at_lap_end": "Tire age",
        }),
        hide_index=True,
        width="stretch",
    )

with tires_tab:
    st.subheader("Strategy and degradation")
    st.markdown(
        "<div class='compound-row'>" + "".join(
            f"<span><i class='compound-dot' style='background:{colour}'></i>{compound.title()}</span>"
            for compound, colour in COMPOUND_COLOURS.items()
        ) + "</div>",
        unsafe_allow_html=True,
    )
    try:
        tires = load_tires(selected_session, driver_numbers)
    except Exception as exc:
        st.error(f"Could not load tire data: {exc}")
        tires = pd.DataFrame()
    if tires.empty:
        st.info("No clean tire-stint laps are available for this selection.")
    else:
        st.plotly_chart(tire_degradation_chart(tires), width="stretch", config={"displayModeBar": False})

with track_tab:
    st.subheader("Minimum-speed signatures")
    st.markdown('<p class="section-note">V-min points show where each lap reached its slowest speed on track.</p>', unsafe_allow_html=True)
    try:
        v_min = load_v_min(selected_session, driver_numbers)
    except Exception as exc:
        st.error(f"Could not load V-min data: {exc}")
        v_min = pd.DataFrame()

    if not v_min.empty:
        v_min = v_min.merge(
            drivers[["driver_number", "full_name", "name_acronym", "team_colour_hex"]],
            on="driver_number",
            how="left",
        )
        st.plotly_chart(v_min_track_chart(v_min), width="stretch", config={"displayModeBar": False})

    st.subheader("Single-lap telemetry")
    telemetry_columns = st.columns(2)
    telemetry_driver = telemetry_columns[0].selectbox(
        "Telemetry driver",
        options=list(driver_numbers),
        format_func=driver_labels.get,
    )
    available_laps = sorted(
        int(number)
        for number in laps.loc[laps["driver_number"] == telemetry_driver, "lap_number"].dropna().unique()
    )
    telemetry_lap = telemetry_columns[1].selectbox("Lap", options=available_laps)
    if telemetry_lap is not None:
        try:
            telemetry = load_telemetry(selected_session, telemetry_driver, telemetry_lap)
        except Exception as exc:
            st.error(f"Could not load telemetry: {exc}")
            telemetry = pd.DataFrame()
        if telemetry.empty:
            st.info("No telemetry samples exist for this lap.")
        else:
            st.plotly_chart(telemetry_chart(telemetry), width="stretch", config={"displayModeBar": False})
