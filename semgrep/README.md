# Semgrep MCP Tool

This is a Python project based on MCP, providing Semgrep functionality. Follow the instructions below to run it.

## Requirements

1. Python >= 3.11

## Run

To start the MCP project and run the semgrep-mcp.py script, use the following command:
uvx mcpo [MCP server options] -- uv [Python options] run [path to semgrep-mcp.py]

ex: uvx mcpo --port 38001 --api-key "eventest" -- uv --directory /home/ecpaas/even/semgrep run semgrep-mcp.py

