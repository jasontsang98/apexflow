import math

import pandas as pd


def format_lap_time(seconds: float | int | None) -> str:
    if seconds is None or pd.isna(seconds):
        return "—"
    minutes, remainder = divmod(float(seconds), 60)
    return f"{int(minutes)}:{remainder:06.3f}"


def fastest_laps(laps: pd.DataFrame) -> pd.DataFrame:
    if laps.empty:
        return laps.copy()
    clean = laps.dropna(subset=["lap_duration"])
    indexes = clean.groupby("driver_number")["lap_duration"].idxmin()
    return clean.loc[indexes].sort_values("lap_duration").reset_index(drop=True)


def overview_metrics(laps: pd.DataFrame) -> dict[str, str]:
    if laps.empty:
        return {
            "fastest_lap": "—",
            "fastest_driver": "—",
            "top_speed": "—",
            "lap_count": "0",
        }

    fastest_index = laps["lap_duration"].idxmin()
    fastest = laps.loc[fastest_index]
    top_speed = laps["top_speed"].max()
    return {
        "fastest_lap": format_lap_time(fastest["lap_duration"]),
        "fastest_driver": str(fastest["name_acronym"]),
        "top_speed": f"{int(round(top_speed))} km/h" if not pd.isna(top_speed) else "—",
        "lap_count": f"{len(laps):,}",
    }


def add_elapsed_time(telemetry: pd.DataFrame) -> pd.DataFrame:
    result = telemetry.copy()
    if result.empty:
        result["elapsed_seconds"] = pd.Series(dtype=float)
        return result
    timestamps = pd.to_datetime(result["telemetry_timestamp"], utc=True, format="mixed")
    result["elapsed_seconds"] = (timestamps - timestamps.min()).dt.total_seconds()
    return result


def safe_axis_range(values: pd.Series, padding: float = 0.05) -> tuple[float, float] | None:
    clean = values.dropna()
    if clean.empty:
        return None
    minimum = float(clean.min())
    maximum = float(clean.max())
    if math.isclose(minimum, maximum):
        return minimum - 1, maximum + 1
    margin = (maximum - minimum) * padding
    return minimum - margin, maximum + margin
