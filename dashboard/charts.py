import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from .analytics import add_elapsed_time, fastest_laps, safe_axis_range

PLOT_BACKGROUND = "rgba(0,0,0,0)"
TEXT_COLOUR = "#d9dde7"
GRID_COLOUR = "rgba(255,255,255,0.08)"
COMPOUND_COLOURS = {
    "SOFT": "#ff3154",
    "MEDIUM": "#ffd33d",
    "HARD": "#eef1f5",
    "INTERMEDIATE": "#39d98a",
    "WET": "#3d8bff",
}


def _style(figure: go.Figure, height: int = 430) -> go.Figure:
    figure.update_layout(
        height=height,
        margin=dict(l=20, r=20, t=50, b=20),
        paper_bgcolor=PLOT_BACKGROUND,
        plot_bgcolor=PLOT_BACKGROUND,
        font=dict(color=TEXT_COLOUR, family="Inter, sans-serif"),
        legend_title_text="",
        hoverlabel=dict(bgcolor="#171923", font_color="#ffffff"),
    )
    figure.update_xaxes(gridcolor=GRID_COLOUR, zerolinecolor=GRID_COLOUR)
    figure.update_yaxes(gridcolor=GRID_COLOUR, zerolinecolor=GRID_COLOUR)
    return figure

def season_races_chart(races: pd.DataFrame) -> go.Figure:
    data = races.sort_values("session_start")
    data = data.assign(
        conditions=data["was_wet"].map({True: "Wet", False: "Dry"}).fillna("Unknown")
    )
    figure = px.bar(
        data,
        x="meeting_name",
        y="fastest_lap",
        color="conditions",
        color_discrete_map={"Dry": "#ff3154", "Wet": "#3d8bff", "Unknown": "#9399aa"},
        custom_data=["circuit_short_name", "fastest_driver", "lap_count", "driver_count"],
        title="Fastest lap at each analysed race",
    )
    figure.update_traces(
        hovertemplate=(
            "<b>%{x}</b><br>%{customdata[0]}<br>"
            "Fastest %{y:.3f}s · %{customdata[1]}<br>"
            "%{customdata[2]} laps · %{customdata[3]} drivers<extra></extra>"
        )
    )
    figure.update_xaxes(title=None, tickangle=-20)
    figure.update_yaxes(title="Fastest lap (seconds)")
    return _style(figure, 470)


def season_driver_chart(drivers: pd.DataFrame) -> go.Figure:
    data = drivers.sort_values("average_delta_to_best", ascending=False)
    figure = px.bar(
        data,
        x="average_delta_to_best",
        y="name_acronym",
        orientation="h",
        color="team_colour_hex",
        color_discrete_map="identity",
        custom_data=["full_name", "team_name", "races", "laps", "top_speed"],
        title="Season pace consistency",
    )
    figure.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>%{customdata[1]}<br>"
            "Average delta %{x:.3f}s<br>%{customdata[2]} races · %{customdata[3]} laps · "
            "%{customdata[4]} km/h top speed<extra></extra>"
        )
    )
    figure.update_layout(showlegend=False)
    figure.update_xaxes(title="Average delta to personal best (s)")
    figure.update_yaxes(title=None)
    return _style(figure, max(430, len(data) * 26))


def fastest_lap_chart(laps: pd.DataFrame) -> go.Figure:
    data = fastest_laps(laps).sort_values("lap_duration", ascending=False)
    figure = px.bar(
        data,
        x="lap_duration",
        y="name_acronym",
        orientation="h",
        color="team_colour_hex",
        color_discrete_map="identity",
        custom_data=["full_name", "team_name", "lap_number", "tire_compound"],
        title="Fastest lap by driver",
    )
    figure.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>%{customdata[1]}<br>"
            "Lap %{customdata[2]} · %{x:.3f}s · %{customdata[3]}<extra></extra>"
        )
    )
    figure.update_layout(showlegend=False)
    figure.update_xaxes(title="Lap duration (seconds)")
    figure.update_yaxes(title=None)
    return _style(figure, max(430, len(data) * 26))


def lap_delta_chart(laps: pd.DataFrame) -> go.Figure:
    figure = px.line(
        laps.sort_values("lap_number"),
        x="lap_number",
        y="delta_to_driver_best",
        color="name_acronym",
        color_discrete_map=dict(zip(laps["name_acronym"], laps["team_colour_hex"])),
        markers=True,
        custom_data=["full_name", "lap_duration", "tire_compound", "tire_age_at_lap_end"],
        title="Lap-time evolution",
    )
    figure.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>Lap %{x}<br>"
            "Delta %{y:.3f}s · Lap %{customdata[1]:.3f}s<br>"
            "%{customdata[2]} · Tire age %{customdata[3]}<extra></extra>"
        )
    )
    figure.update_xaxes(title="Lap")
    figure.update_yaxes(title="Delta to driver best (s)")
    return _style(figure)


def tire_degradation_chart(tires: pd.DataFrame) -> go.Figure:
    figure = px.line(
        tires.sort_values("lap_in_stint"),
        x="lap_in_stint",
        y="delta_to_stint_best",
        color="name_acronym",
        line_dash="tire_compound",
        markers=True,
        custom_data=["full_name", "lap_number", "tire_compound", "tire_age_at_lap_end", "strategy_stint_number"],
        title="Tire degradation by inferred stint",
    )
    figure.update_traces(
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>Race lap %{customdata[1]} · Stint %{customdata[4]}<br>"
            "%{customdata[2]} age %{customdata[3]}<br>Delta %{y:.3f}s<extra></extra>"
        )
    )
    figure.update_xaxes(title="Lap in stint")
    figure.update_yaxes(title="Delta to stint best (s)")
    return _style(figure)


def v_min_track_chart(points: pd.DataFrame) -> go.Figure:
    figure = px.scatter(
        points,
        x="x",
        y="y",
        color="lap_v_min",
        symbol="name_acronym",
        color_continuous_scale="Turbo",
        custom_data=["full_name", "lap_number", "lap_v_min", "v_min_gear", "tire_compound"],
        title="Where each lap reached minimum speed",
    )
    figure.update_traces(
        marker=dict(size=9, opacity=0.78),
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>Lap %{customdata[1]} · %{customdata[2]} km/h<br>"
            "Gear %{customdata[3]} · %{customdata[4]}<extra></extra>"
        ),
    )
    figure.update_layout(coloraxis_colorbar_title="V-min<br>km/h")
    figure.update_xaxes(title=None, visible=False, scaleanchor="y")
    figure.update_yaxes(title=None, visible=False)
    return _style(figure, 560)


def telemetry_chart(telemetry: pd.DataFrame) -> go.Figure:
    data = add_elapsed_time(telemetry)
    figure = make_subplots(specs=[[{"secondary_y": True}]])
    figure.add_trace(
        go.Scatter(
            x=data["elapsed_seconds"],
            y=data["speed"],
            name="Speed",
            line=dict(color="#ff3154", width=2.5),
        ),
        secondary_y=False,
    )
    figure.add_trace(
        go.Scatter(
            x=data["elapsed_seconds"],
            y=data["throttle"],
            name="Throttle",
            line=dict(color="#39d98a", width=1.5),
            opacity=0.8,
        ),
        secondary_y=True,
    )
    figure.add_trace(
        go.Scatter(
            x=data["elapsed_seconds"],
            y=data["brake"] * 100,
            name="Brake",
            line=dict(color="#ffd33d", width=1.5),
            opacity=0.8,
        ),
        secondary_y=True,
    )
    figure.update_layout(title="Speed, throttle and brake trace")
    figure.update_xaxes(title="Elapsed lap time (s)")
    figure.update_yaxes(title="Speed (km/h)", secondary_y=False)
    figure.update_yaxes(title="Pedal input (%)", range=[0, 105], secondary_y=True)
    speed_range = safe_axis_range(data["speed"])
    if speed_range:
        figure.update_yaxes(range=speed_range, secondary_y=False)
    return _style(figure, 480)
