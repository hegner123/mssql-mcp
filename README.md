# mssql-mcp

An MCP (Model Context Protocol) server that connects to Microsoft SQL Server databases. Auto-discovers connection strings from .NET `appsettings.json` files so AI assistants can query and explore MSSQL databases without manual configuration.

## Features

- Auto-discovers MSSQL connection strings from `appsettings.json` and `appsettings.*.json` files
- Finds connections in both `ConnectionStrings` sections and top-level keys (e.g. `umbracoDbDSN`)
- Four tools: discover connections, execute queries, list tables, describe table schemas
- Supports ADO.NET-style connection strings (`Server=host,port;Database=db;User Id=...`)
- Works as both an MCP server (stdio transport) and a standalone CLI tool

## Installation

```bash
go install github.com/hegner123/mssql-mcp@latest
```

Or build from source:

```bash
git clone https://github.com/hegner123/mssql-mcp.git
cd mssql-mcp
just install
```

## Usage

### MCP Server

Add to your Claude Code configuration:

```bash
claude mcp add --transport stdio --scope user mssql-mcp -- mssql-mcp
```

Or add to `.mcp.json`:

```json
{
  "mcpServers": {
    "mssql-mcp": {
      "command": "mssql-mcp"
    }
  }
}
```

### Tools

**mssql_discover** - Find connection strings in appsettings files:

```bash
mssql-mcp --cli --tool discover --dir /path/to/dotnet/project
```

**mssql_query** - Execute a SQL query:

```bash
mssql-mcp --cli --tool query --query "SELECT TOP 10 * FROM Users"
```

**mssql_tables** - List all tables:

```bash
mssql-mcp --cli --tool tables --connection-string "Server=localhost,1433;Database=mydb;User Id=SA;Password=pass;TrustServerCertificate=True"
```

**mssql_schema** - Describe a table's columns, types, and keys:

```bash
mssql-mcp --cli --tool schema --table "dbo.Users"
```

All tools accept `--connection-string` to provide an explicit connection, or `--dir` and `--key` to auto-discover from appsettings files. If no connection string is provided, the tool searches the current directory for appsettings files automatically.

### Connection String Format

Standard ADO.NET format:

```
Server=localhost,1433;Database=ugm;User Id=SA;Password=Root#Root12;TrustServerCertificate=True
```

The tool also accepts `sqlserver://` URL format directly.

## License

[MIT](LICENSE)
