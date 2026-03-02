import asyncio
import json
from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client, StdioServerParameters


async def test():
    server = StdioServerParameters(
        command="python",
        args=["mcp_server.py"],
    )

    async with stdio_client(server) as (read, write):
        async with ClientSession(read, write) as session:

            # 🔥 REQUIRED STEP
            await session.initialize()

            print("🔍 Checking available tools...")
            tools = await session.list_tools()
            print("Tools:", tools)

            print("\n📂 Checking sandbox files...")
            files = await session.read_resource("sandbox://files")
            print(files)

            print("\n🚀 Calling ingest tool...")
            response = await session.call_tool(
                "ingest",
                {"path": "sandbox/customers-100.csv"}
            )
            
            print("\n📊 CLEAN OUTPUT:\n")

            if response.isError:
               print("❌ Error:", response.content[0].text)
            else:
            # Extract JSON string
               raw_json = response.content[0].text
            
            # Convert string → dict
               parsed = json.loads(raw_json)
            
            # Pretty print
               print(json.dumps(parsed, indent=4))


            print("\n✅ MCP RESPONSE end")
            # print(response)


asyncio.run(test())