from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class ColumnProfile(BaseModel):
    name: str
    dtype: str
    missing: int
    unique: int
    min: Optional[float] = None
    max: Optional[float] = None
    mean: Optional[float] = None
    outliers: Optional[int] = None


class QualityReport(BaseModel):
    total_rows: int
    total_columns: int
    duplicate_rows: int
    missing_cells: int
    quality_score: float


class DatasetResponse(BaseModel):
    file_name: str
    columns: List[str]
    row_count: int
    column_profiles: List[ColumnProfile]
    quality_report: QualityReport