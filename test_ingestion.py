from ingestion_engine import ingest_file

if __name__ == "__main__":
    result = ingest_file("sandbox/customers-100.csv")

    print("✅ INGESTION SUCCESS")
    print("File Name:", result["file_name"])
    print("Columns:", result["columns"])
    print("Rows:", result["row_count"])