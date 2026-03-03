from mcp.server.fastmcp import FastMCP
from ingestion_engine import ingest_file
import os
import json
import io
import sys
import time


mcp = FastMCP("AI Analytics Phase 1")



@mcp.tool()
def ingest(path: str) -> dict:
    # Capture print output
    buffer = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = buffer

    try:
        result = ingest_file(path)
    finally:
        sys.stdout = sys_stdout

    logs = buffer.getvalue().strip().split("\n")

    # Include logs in response
    return {
        "status": "success",
        "logs": logs,
        "result": result
    }


@mcp.resource("sandbox://files")
def list_sandbox_files() -> str:
    files = os.listdir("sandbox")
    return "\n".join(files)


@mcp.prompt()
def analyze_file_prompt(file_name: str) -> str:
    return f"""
You are a data analyst.
Analyze the dataset {file_name}.
Provide insights, trends, and data quality interpretation.
"""


if __name__ == "__main__":
    mcp.run()