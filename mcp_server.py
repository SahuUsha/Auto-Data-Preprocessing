from mcp.server.fastmcp import FastMCP
from ingestion_engine import ingest_file
import os
import json

mcp = FastMCP("AI Analytics Phase 1")


@mcp.tool()
def ingest(path: str) -> dict:
    """
    Ingest a CSV, Excel, or JSON file.
    Performs normalization and profiling.
    """

    result = ingest_file(path)

    # return {
    #     "content": [
    #         {
    #             "type": "text",
    #             "text": json.dumps(result, indent=2)
    #         }
    #     ]
    # }
    
    return result


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