import json
import asyncio
from fastapi import FastAPI
from pydantic import BaseModel
from openai import OpenAI
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client

app = FastAPI()
client = OpenAI()


class AnalyzeRequest(BaseModel):
    path: str
    message: str


@app.post("/analyze")
async def analyze(request: AnalyzeRequest):

    # 1️⃣ Ask LLM what to do
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a data analyst."},
            {"role": "user", "content": request.message}
        ],
        tools=[
            {
                "type": "function",
                "function": {
                    "name": "ingest",
                    "description": "Ingest dataset file",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "path": {"type": "string"}
                        },
                        "required": ["path"]
                    }
                }
            }
        ],
        tool_choice="auto"
    )

    message = response.choices[0].message

    # 2️⃣ If LLM calls tool
    if message.tool_calls:

        tool_call = message.tool_calls[0]
        args = json.loads(tool_call.function.arguments)

        # 3️⃣ Connect to MCP server
        async with stdio_client("python mcp_server.py") as (read, write):
            async with ClientSession(read, write) as session:
                result = await session.call_tool("ingest", args)

        # 4️⃣ Send result back to LLM
        final_response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a data analyst."},
                {"role": "user", "content": request.message},
                message,
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(result)
                }
            ]
        )

        return {"analysis": final_response.choices[0].message.content}

    return {"analysis": message.content}