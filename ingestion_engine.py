# import os
# import time
# import pandas as pd
# import json
# import shutil
# import tempfile
# from pathlib import Path
# import magic



# MAX_FILE_SIZE_MB = 500


# def validate_file_size(path: str):
#     size_mb = Path(path).stat().st_size / (1024 * 1024)
#     if size_mb > MAX_FILE_SIZE_MB:
#         raise ValueError("File exceeds 500MB limit")


# def copy_to_temp(path: str) -> str:
#     temp_dir = tempfile.mkdtemp()
#     temp_path = os.path.join(temp_dir, os.path.basename(path))
#     shutil.copy(path, temp_path)
#     return temp_path


# def detect_file_type(path: str) -> str:
#     mime = magic.from_file(path, mime=True)

#     if "csv" in mime or path.endswith(".csv"):
#         return "csv"
#     if "excel" in mime or path.endswith((".xls", ".xlsx")):
#         return "excel"
#     if "json" in mime or path.endswith(".json"):
#             return "json"

#     return "unknown"


# def read_file(path: str, file_type: str):

#     if file_type == "csv":
#         return pd.read_csv(path, dtype=str)

#     if file_type == "excel":
#         return pd.read_excel(path, dtype=str)

#     if file_type == "json":
#         with open(path, "r", encoding="utf-8") as f:
#             data = json.load(f)
#         return pd.json_normalize(data)

#     raise ValueError("Unsupported file format")


# def detect_header(df: pd.DataFrame):

#     df = df.dropna(how="all")

#     if df.empty:
#         raise ValueError("Dataset contains no data rows")

#     # If pandas auto-generated numeric column names
#     if all(str(col).isdigit() for col in df.columns):
#         df.columns = df.iloc[0]
#         df = df[1:]

#     return df.reset_index(drop=True)


# def basic_cleaning(df: pd.DataFrame):

#     # Remove fully empty rows
#     df = df.dropna(how="all")
    
#     df.columns = (
#     df.columns
#     .str.strip()
#     .str.lower()
#     .str.replace(" ", "_", regex=False)
# )

#     # Trim whitespace only for string columns (vectorized + safe)
#     str_cols = df.select_dtypes(include=["object"]).columns
#     df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

#     # Convert empty strings to None
#     df = df.replace("", None)

#     # Remove duplicates
#     df = df.drop_duplicates()

#     return df.reset_index(drop=True)

#     # # Remove fully empty rows
#     # df = df.dropna(how="all")

#     # # Trim whitespace
#     # df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

#     # # Convert empty strings to None
#     # df = df.replace("", None)

#     # # Remove duplicates
#     # df = df.drop_duplicates()

#     # return df.reset_index(drop=True)


# def ingest_file(file_path: str):

#     start = time.time()
#     print("Start ingest")

#     # 1. Validate file size
#     validate_file_size(file_path)

#     # 2. Copy to temp (preserve original)
#     # temp_path = copy_to_temp(file_path)
#     temp_path = file_path

#     # 3. Detect file type
#     file_type = detect_file_type(temp_path)

#     if file_type == "unknown":
#         raise ValueError("Unsupported file format")

#     # 4. Read file
#     df = read_file(temp_path, file_type)
#     print("Read:", time.time() - start)

#     # 5. Detect header
#     start = time.time()
#     df = basic_cleaning(df)
#     print("Clean:", time.time() - start)


#     # 6. Basic preprocessing
#     df = basic_cleaning(df)

#     # 7. Final validation
#     if df.empty:
#         raise ValueError("No valid data rows after preprocessing")

#     return {
#         "file_name": os.path.basename(file_path),
#         "columns": list(df.columns),
#         "row_count": len(df),
#         "column_count": len(df.columns),
#         "data": df.to_dict(orient="records")
#     }



import os
import time
import pandas as pd
import json
from pathlib import Path

MAX_FILE_SIZE_MB = 500




# ----------------------------
# Utility Logger
# ----------------------------

def log_step(title):
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")


# ----------------------------
# File Validation
# ----------------------------

def validate_file_size(path: str):
    size_bytes = Path(path).stat().st_size
    size_mb = size_bytes / (1024 * 1024)

    print(f"File size: {round(size_mb, 4)} MB")

    if size_mb > MAX_FILE_SIZE_MB:
        raise ValueError("File exceeds 500MB limit")


# ----------------------------
# File Type Detection
# ----------------------------

def detect_file_type(path: str) -> str:
    ext = Path(path).suffix.lower()
    print(f"Detected extension: {ext}")

    if ext == ".csv":
        return "csv"
    if ext in [".xls", ".xlsx"]:
        return "excel"
    if ext == ".json":
        return "json"

    return "unknown"


# ----------------------------
# File Reader
# ----------------------------

def read_file(path: str, file_type: str):

    print(f"Reading file as: {file_type.upper()}")

    if file_type == "csv":
        return pd.read_csv(path, dtype=str)

    if file_type == "excel":
        return pd.read_excel(path, dtype=str, engine="openpyxl")

    if file_type == "json":
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return pd.json_normalize(data)

    raise ValueError("Unsupported file format")


# ----------------------------
# Header Detection
# ----------------------------

def detect_header(df: pd.DataFrame):

    print("Checking header structure...")

    before_rows = len(df)

    df = df.dropna(how="all")

    if df.empty:
        raise ValueError("Dataset contains no data rows")

    if all(str(col).isdigit() for col in df.columns):
        print("Numeric column headers detected. Promoting first row to header.")
        df.columns = df.iloc[0]
        df = df[1:]

    after_rows = len(df)

    print(f"Rows before header fix: {before_rows}")
    print(f"Rows after header fix:  {after_rows}")

    return df.reset_index(drop=True)


# ----------------------------
# Cleaning
# ----------------------------

def basic_cleaning(df: pd.DataFrame):

    print("\nStarting data cleaning...")

    original_rows = len(df)

    # Remove fully empty rows
    empty_rows = df.isnull().all(axis=1).sum()
    df = df.dropna(how="all")

    print(f"Fully empty rows removed: {empty_rows}")

    # Clean column names
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    # Strip string columns
    str_cols = df.select_dtypes(include=["object"]).columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    df = df.replace("", None)

    # -------------------------
    # 🔎 NULL ANALYSIS
    # -------------------------
    print("\n NULL VALUE ANALYSIS")
    null_counts = df.isnull().sum()
    null_percent = (df.isnull().mean() * 100).round(2)

    for col in df.columns:
        print(f"{col}: {null_counts[col]} nulls ({null_percent[col]}%)")

    # -------------------------
    # 🔎 DUPLICATE ANALYSIS
    # -------------------------
    print("\n DUPLICATE ANALYSIS")

    total_duplicates = df.duplicated().sum()
    print(f"Total duplicate rows: {total_duplicates}")

    for col in df.columns:
        col_dup = df[col].duplicated().sum()
        print(f"{col}: {col_dup} duplicate values")

    # -------------------------
    # 🧹 HANDLE DUPLICATES
    # -------------------------
    df = df.drop_duplicates()
    print(f"Duplicate rows removed: {total_duplicates}")

    # -------------------------
    # 🧹 HANDLE NULLS (Strategy)
    # -------------------------

    # Example Strategy:
    # Drop columns if >60% null
    threshold = 60
    cols_to_drop = null_percent[null_percent > threshold].index.tolist()

    if cols_to_drop:
        print(f"\nDropping columns with >{threshold}% null values:")
        print(cols_to_drop)
        df = df.drop(columns=cols_to_drop)

    # Optional: Fill remaining nulls with placeholder
    df = df.fillna("Unknown")

    print("\nRemaining rows:", len(df))

    return df.reset_index(drop=True)

# ----------------------------
# Profiling
# ----------------------------

def generate_profiling(df: pd.DataFrame):

    print("\nGenerating column profiling...")

    profile = {}

    for col in df.columns:
        profile[col] = {
            "dtype": str(df[col].dtype),
            "unique_values": int(df[col].nunique()),
            "top_values": df[col].value_counts().head(3).to_dict()
        }

    return profile


# ----------------------------
# Insights Generator
# ----------------------------

def generate_insights(df: pd.DataFrame):

    print("\nGenerating automatic insights...")

    insights = []

    for col in df.columns:

        unique_ratio = df[col].nunique() / len(df)

        if unique_ratio > 0.9:
            insights.append(f"{col} appears to be a high-cardinality column (likely ID)")

        null_percent = (df[col] == "Unknown").mean() * 100

        if null_percent > 40:
            insights.append(f"{col} contains {round(null_percent,2)}% missing values")

    return insights


# ----------------------------
# Main Ingestion Pipeline
# ----------------------------

def ingest_file(file_path: str):

    total_start = time.time()

    log_step("STEP 1: FILE VALIDATION")
    validate_file_size(file_path)

    log_step("STEP 2: FILE TYPE DETECTION")
    file_type = detect_file_type(file_path)

    if file_type == "unknown":
        raise ValueError("Unsupported file format")

    log_step("STEP 3: FILE READING")
    read_start = time.time()
    df = read_file(file_path, file_type)
    print("Read time:", round(time.time() - read_start, 4), "seconds")
    print("Initial shape:", df.shape)

    log_step("STEP 4: HEADER DETECTION")
    df = detect_header(df)
    print("Shape after header fix:", df.shape)

    log_step("STEP 5: DATA CLEANING + QUALITY CHECK")
    df = basic_cleaning(df)
    print("Shape after cleaning:", df.shape)

    if df.empty:
        raise ValueError("No valid data rows after preprocessing")

    log_step("STEP 6: PROFILING")
    profiling = generate_profiling(df)

    log_step("STEP 7: AUTO INSIGHTS")
    insights = generate_insights(df)

    print("\nTotal pipeline time:",
          round(time.time() - total_start, 4), "seconds")

    # ----------------------------
    # FINAL STRUCTURED RESPONSE
    # ----------------------------

    return {
        "file_name": os.path.basename(file_path),
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "profiling": profiling,
        "insights": insights,
        "preview": df.head(5).to_dict(orient="records")
    }