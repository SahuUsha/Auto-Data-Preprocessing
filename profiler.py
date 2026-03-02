import pandas as pd
from models import ColumnProfile, QualityReport


def detect_outliers(series):
    if not pd.api.types.is_numeric_dtype(series):
        return 0

    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1

    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr

    return int(((series < lower) | (series > upper)).sum())


def profile_dataframe(df):

    profiles = []
    total_missing = int(df.isna().sum().sum())
    duplicate_rows = int(df.duplicated().sum())

    for col in df.columns:
        series = df[col]

        profile = ColumnProfile(
            name=col,
            dtype=str(series.dtype),
            missing=int(series.isna().sum()),
            unique=int(series.nunique()),
        )

        if pd.api.types.is_numeric_dtype(series):
            profile.min = float(series.min())
            profile.max = float(series.max())
            profile.mean = float(series.mean())
            profile.outliers = detect_outliers(series)

        profiles.append(profile)

    total_cells = df.shape[0] * df.shape[1]
    score = 1 - ((total_missing / total_cells) + (duplicate_rows / len(df))) / 2
    score = round(max(score, 0), 2)

    quality = QualityReport(
        total_rows=len(df),
        total_columns=len(df.columns),
        duplicate_rows=duplicate_rows,
        missing_cells=total_missing,
        quality_score=score,
    )

    return profiles, quality