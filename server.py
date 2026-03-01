"""
MCP Database Connector Lite — Free SQLite-only MCP Server
For the full version with PostgreSQL, MySQL, connection pooling, and safety controls:
→ https://whop.com/tirantech

Part of the MCP Starter Arsenal by TiranTech.
"""

import asyncio
import json
import logging
import os
import sqlite3
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

logger = logging.getLogger(__name__)


def build_server(db_path: str = None) -> Server:
    db_path = db_path or os.getenv("MCP_DB_DATABASE", "data.db")
    server = Server("mcp-database-connector-lite")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="db_query",
                description="Execute a SQL query against the SQLite database.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "SQL query to execute"},
                    },
                    "required": ["query"],
                },
            ),
            Tool(
                name="db_list_tables",
                description="List all tables in the database.",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="db_describe_table",
                description="Get the schema of a table.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "table": {"type": "string", "description": "Table name"},
                    },
                    "required": ["table"],
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        try:
            if name == "db_query":
                cursor = conn.execute(arguments["query"])
                if cursor.description:
                    columns = [d[0] for d in cursor.description]
                    rows = cursor.fetchmany(500)
                    results = [dict(zip(columns, row)) for row in rows]
                    return [TextContent(type="text", text=json.dumps({"results": results, "count": len(results)}, default=str))]
                conn.commit()
                return [TextContent(type="text", text=json.dumps({"affected_rows": cursor.rowcount}))]

            elif name == "db_list_tables":
                cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                tables = [row[0] for row in cursor.fetchall()]
                return [TextContent(type="text", text=json.dumps({"tables": tables}))]

            elif name == "db_describe_table":
                cursor = conn.execute(f"PRAGMA table_info({arguments['table']})")
                columns = [{"name": r[1], "type": r[2], "nullable": not r[3], "primary_key": bool(r[5])} for r in cursor.fetchall()]
                return [TextContent(type="text", text=json.dumps({"table": arguments["table"], "columns": columns}))]

            return [TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]
        except Exception as e:
            return [TextContent(type="text", text=json.dumps({"error": str(e)}))]

    return server


async def main():
    server = build_server()
    logger.info("MCP Database Connector Lite started (SQLite only)")
    logger.info("For PostgreSQL, MySQL, pooling & safety → https://whop.com/tirantech")

    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
