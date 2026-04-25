from pydantic import BaseModel, Field, ConfigDict, field_validator, field_validator
from typing import Optional, List
from datetime import datetime

class TelemetryData(BaseModel):
    """
    Corrected Schema for OpenF1 car_data (3.7Hz)
    The API uses 'n_gear' for the current gear selection.
    """
    # Use Field(alias=...) if you want to keep the name 'gear' in your code
    # while reading 'n_gear' from the API.
    date: datetime
    session_key: int
    meeting_key: int
    driver_number: int
    speed: int
    rpm: int
    n_gear: int
    throttle: int
    brake: int
    drs: int

class LapData(BaseModel):
    """Schema for OpenF1 laps endpoint"""
    session_key: int
    meeting_key: int
    driver_number: int
    lap_number: int
    date_start: datetime
    lap_duration: Optional[float] = None
    duration_sector_1: Optional[float] = None
    duration_sector_2: Optional[float] = None
    duration_sector_3: Optional[float] = None
    segments_sector_1: Optional[List[float]] = Field(default_factory=list)
    segments_sector_2: Optional[List[float]] = Field(default_factory=list)
    segments_sector_3: Optional[List[float]] = Field(default_factory=list)
    st_speed: Optional[int] = None
    is_pit_out_lap: bool
    
    # NEW: The "Null Stripper" Validator
    @field_validator('segments_sector_1', 'segments_sector_2', 'segments_sector_3', mode='before')
    @classmethod
    def remove_nulls_from_segments(cls, v: List) -> List:
        if v is None:
            return []
        # Filter out any None values so BigQuery doesn't crash
        return [x for x in v if x is not None]

class StintData(BaseModel):
    session_key: int
    meeting_key: int
    driver_number: int
    stint_number: int
    compound: str
    tyre_age_at_start: int
    lap_start: Optional[int] = None
    lap_end: Optional[int] = None
    
class PitData(BaseModel):
    session_key: int
    meeting_key: int
    driver_number: int
    date: datetime
    lap_number: int
    lane_duration: Optional[float] = None
    stop_duration: Optional[float] = None

class PositionData(BaseModel):
    date: datetime
    driver_number: int
    session_key: int
    meeting_key: int
    position: int

class LocationData(BaseModel):
    date: datetime
    driver_number: int
    session_key: int
    x: float
    y: float
    z: float

# Metadata Schemas
class MeetingData(BaseModel):
    """Schema for Grand Prix event metadata"""
    circuit_key: int
    circuit_short_name: str
    circuit_type: str
    country_name: str
    meeting_key: int
    meeting_name: str
    location: str
    date_start: datetime
    date_end: Optional[datetime] = None
    year: int
    is_cancelled: bool

class SessionData(BaseModel):
    """Schema for session-level metadata (Practice, Quali, Race)"""
    session_key: int
    session_name: str
    session_type: str
    meeting_key: int
    date_start: datetime
    date_end: Optional[datetime] = None
    year: int

class DriverData(BaseModel):
    """Schema for Driver details and team affiliation"""
    driver_number: int
    broadcast_name: str
    full_name: str
    name_acronym: str
    team_name: str
    team_colour: str
    session_key: int

class WeatherData(BaseModel):
    meeting_key: int
    session_key: int
    date: datetime
    air_temperature: float
    track_temperature: float
    humidity: float
    pressure: float
    wind_direction: int
    wind_speed: float
    rainfall: bool
    
class RaceControlData(BaseModel):
    session_key: int
    meeting_key: int
    date: datetime
    category: str
    flag: Optional[str] = None
    lap_number: Optional[int] = None
    message: str
    sector: Optional[int] = None
    driver_number: Optional[int] = None
    scope: Optional[str] = None