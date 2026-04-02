package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	_ "github.com/microsoft/go-mssqldb"
)

const (
	ExitSuccess = 0
	ExitError   = 1
)

// ---------- Result types ----------

type DiscoverResult struct {
	Connections []FoundConnection `json:"connections"`
	Count       int               `json:"count"`
}

type FoundConnection struct {
	File string `json:"file"`
	Key  string `json:"key"`
	DSN  string `json:"dsn"`
}

type QueryResult struct {
	Columns  []string         `json:"columns"`
	Rows     []map[string]any `json:"rows"`
	RowCount int              `json:"row_count"`
}

type TablesResult struct {
	Tables []TableInfo `json:"tables"`
	Count  int         `json:"count"`
}

type TableInfo struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
	Type   string `json:"type"`
}

type SchemaResult struct {
	Table   string       `json:"table"`
	Columns []ColumnInfo `json:"columns"`
}

type ColumnInfo struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`
	MaxLength  any    `json:"max_length"`
	IsNullable bool   `json:"is_nullable"`
	Default    string `json:"default,omitempty"`
	IsPK       bool   `json:"is_primary_key"`
}

// ---------- MCP JSON-RPC types ----------

type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type JSONRPCResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  any    `json:"result,omitempty"`
	Error   *Error `json:"error,omitempty"`
}

type Error struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type InitializeResult struct {
	ProtocolVersion string       `json:"protocolVersion"`
	ServerInfo      ServerInfo   `json:"serverInfo"`
	Capabilities    Capabilities `json:"capabilities"`
}

type ServerInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type Capabilities struct {
	Tools map[string]bool `json:"tools"`
}

type ToolsListResult struct {
	Tools []Tool `json:"tools"`
}

type Tool struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	InputSchema InputSchema `json:"inputSchema"`
}

type InputSchema struct {
	Type       string              `json:"type"`
	Properties map[string]Property `json:"properties"`
	Required   []string            `json:"required"`
}

type Property struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	Default     any    `json:"default,omitempty"`
}

type ToolCallParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type ToolCallResult struct {
	Content []ContentItem `json:"content"`
}

type ContentItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// ---------- main ----------

func main() {
	cliMode := flag.Bool("cli", false, "Run in CLI mode")
	cliTool := flag.String("tool", "", "Tool to run in CLI mode: discover, query, tables, schema")
	flagConnStr := flag.String("connection-string", "", "MSSQL connection string")
	flagQuery := flag.String("query", "", "SQL query to execute")
	flagDir := flag.String("dir", "", "Directory to search for appsettings.json")
	flagTable := flag.String("table", "", "Table name for schema tool")
	flagKey := flag.String("key", "", "Connection string key name in appsettings.json")
	flag.Parse()

	if *cliMode {
		runCLI(*cliTool, *flagConnStr, *flagQuery, *flagDir, *flagTable, *flagKey)
		return
	}

	runMCPServer()
}

func runCLI(tool, connStr, query, dir, table, key string) {
	if tool == "" {
		fmt.Fprintln(os.Stderr, "Error: --tool is required (discover, query, tables, schema)")
		os.Exit(ExitError)
	}

	var result any
	var err error

	switch tool {
	case "discover":
		result, err = executeDiscover(dir, key)
	case "query":
		if query == "" {
			fmt.Fprintln(os.Stderr, "Error: --query is required")
			os.Exit(ExitError)
		}
		result, err = executeQuery(connStr, query, dir, key)
	case "tables":
		result, err = executeTables(connStr, dir, key)
	case "schema":
		if table == "" {
			fmt.Fprintln(os.Stderr, "Error: --table is required")
			os.Exit(ExitError)
		}
		result, err = executeSchema(connStr, table, dir, key)
	default:
		fmt.Fprintf(os.Stderr, "Error: unknown tool %q\n", tool)
		os.Exit(ExitError)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(ExitError)
	}

	output, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling result: %v\n", err)
		os.Exit(ExitError)
	}
	fmt.Println(string(output))
}

// ---------- MCP Server ----------

func runMCPServer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	scanner := bufio.NewScanner(os.Stdin)
	// Allow large messages (16MB)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	lineChan := make(chan string)
	errChan := make(chan error, 1)

	go func() {
		for scanner.Scan() {
			lineChan <- scanner.Text()
		}
		if err := scanner.Err(); err != nil {
			errChan <- err
		}
		close(lineChan)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errChan:
			fmt.Fprintf(os.Stderr, "Scanner error: %v\n", err)
			return
		case line, ok := <-lineChan:
			if !ok {
				return
			}
			if line == "" {
				continue
			}
			var req JSONRPCRequest
			if err := json.Unmarshal([]byte(line), &req); err != nil {
				sendError(nil, -32700, "Parse error")
				continue
			}
			handleRequest(req)
		}
	}
}

func handleRequest(req JSONRPCRequest) {
	isNotification := req.ID == nil

	switch req.Method {
	case "initialize":
		handleInitialize(req)
	case "notifications/initialized":
		return
	case "tools/list":
		handleToolsList(req)
	case "tools/call":
		handleToolsCall(req)
	default:
		if isNotification {
			return
		}
		sendError(req.ID, -32601, "Method not found")
	}
}

func handleInitialize(req JSONRPCRequest) {
	result := InitializeResult{
		ProtocolVersion: "2024-11-05",
		ServerInfo: ServerInfo{
			Name:    "mssql-mcp",
			Version: "1.0.0",
		},
		Capabilities: Capabilities{
			Tools: map[string]bool{"list": true, "call": true},
		},
	}
	sendResponse(req.ID, result)
}

var toolDefinitions = []Tool{
	{
		Name:        "mssql_discover",
		Description: "Discover MSSQL connection strings in appsettings.json and appsettings.*.json files in a directory.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]Property{
				"dir": {
					Type:        "string",
					Description: "Directory to search. Defaults to current working directory.",
				},
				"key": {
					Type:        "string",
					Description: "Filter to a specific connection string key name (e.g. 'umbracoDbDSN').",
				},
			},
			Required: []string{},
		},
	},
	{
		Name:        "mssql_query",
		Description: "Execute a SQL query against a Microsoft SQL Server database. Returns columns and rows as JSON.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]Property{
				"query": {
					Type:        "string",
					Description: "SQL query to execute.",
				},
				"connection_string": {
					Type:        "string",
					Description: "MSSQL connection string (e.g. Server=localhost,1433;Database=db;User Id=SA;Password=pass;TrustServerCertificate=True). If omitted, auto-discovers from appsettings.json.",
				},
				"dir": {
					Type:        "string",
					Description: "Directory to search for appsettings.json when auto-discovering connection string.",
				},
				"key": {
					Type:        "string",
					Description: "Connection string key name to use from appsettings.json (e.g. 'umbracoDbDSN'). Uses first found if omitted.",
				},
			},
			Required: []string{"query"},
		},
	},
	{
		Name:        "mssql_tables",
		Description: "List all user tables in the connected MSSQL database.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]Property{
				"connection_string": {
					Type:        "string",
					Description: "MSSQL connection string. If omitted, auto-discovers from appsettings.json.",
				},
				"dir": {
					Type:        "string",
					Description: "Directory to search for appsettings.json when auto-discovering.",
				},
				"key": {
					Type:        "string",
					Description: "Connection string key name from appsettings.json.",
				},
			},
			Required: []string{},
		},
	},
	{
		Name:        "mssql_schema",
		Description: "Describe the schema of a specific table (columns, types, nullability, primary keys, defaults).",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]Property{
				"table": {
					Type:        "string",
					Description: "Table name to describe. Can include schema prefix (e.g. 'dbo.Users').",
				},
				"connection_string": {
					Type:        "string",
					Description: "MSSQL connection string. If omitted, auto-discovers from appsettings.json.",
				},
				"dir": {
					Type:        "string",
					Description: "Directory to search for appsettings.json when auto-discovering.",
				},
				"key": {
					Type:        "string",
					Description: "Connection string key name from appsettings.json.",
				},
			},
			Required: []string{"table"},
		},
	},
}

func handleToolsList(req JSONRPCRequest) {
	result := ToolsListResult{Tools: toolDefinitions}
	sendResponse(req.ID, result)
}

func handleToolsCall(req JSONRPCRequest) {
	var params ToolCallParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		sendError(req.ID, -32602, "Invalid params")
		return
	}

	args := params.Arguments
	getStr := func(key string) string {
		v, _ := args[key].(string)
		return v
	}

	var result any
	var err error

	switch params.Name {
	case "mssql_discover":
		result, err = executeDiscover(getStr("dir"), getStr("key"))
	case "mssql_query":
		q := getStr("query")
		if q == "" {
			sendError(req.ID, -32602, "Missing required 'query' parameter")
			return
		}
		result, err = executeQuery(getStr("connection_string"), q, getStr("dir"), getStr("key"))
	case "mssql_tables":
		result, err = executeTables(getStr("connection_string"), getStr("dir"), getStr("key"))
	case "mssql_schema":
		t := getStr("table")
		if t == "" {
			sendError(req.ID, -32602, "Missing required 'table' parameter")
			return
		}
		result, err = executeSchema(getStr("connection_string"), t, getStr("dir"), getStr("key"))
	default:
		sendError(req.ID, -32602, fmt.Sprintf("Unknown tool: %s", params.Name))
		return
	}

	if err != nil {
		sendError(req.ID, -32603, fmt.Sprintf("Execution failed: %v", err))
		return
	}

	jsonResult, err := json.Marshal(result)
	if err != nil {
		sendError(req.ID, -32603, "Failed to marshal result")
		return
	}

	callResult := ToolCallResult{
		Content: []ContentItem{
			{Type: "text", Text: string(jsonResult)},
		},
	}
	sendResponse(req.ID, callResult)
}

func sendResponse(id any, result any) {
	resp := JSONRPCResponse{JSONRPC: "2.0", ID: id, Result: result}
	data, err := json.Marshal(resp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal response: %v\n", err)
		return
	}
	fmt.Println(string(data))
}

func sendError(id any, code int, message string) {
	resp := JSONRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &Error{Code: code, Message: message},
	}
	data, err := json.Marshal(resp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to marshal error: %v\n", err)
		return
	}
	fmt.Println(string(data))
}

// ---------- Connection string discovery ----------

// discoverConnectionStrings finds MSSQL connection strings in appsettings*.json files.
// It looks for "ConnectionStrings" objects and any string value containing "Server=" or "Data Source=".
func discoverConnectionStrings(dir, keyFilter string) ([]FoundConnection, error) {
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("getting working directory: %w", err)
		}
	}

	patterns := []string{
		filepath.Join(dir, "appsettings.json"),
		filepath.Join(dir, "appsettings.*.json"),
	}

	var files []string
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, fmt.Errorf("globbing %s: %w", pattern, err)
		}
		files = append(files, matches...)
	}

	// Deduplicate (appsettings.json matches both patterns on some systems)
	seen := make(map[string]bool)
	var unique []string
	for _, f := range files {
		if !seen[f] {
			seen[f] = true
			unique = append(unique, f)
		}
	}

	var found []FoundConnection
	for _, file := range unique {
		conns, err := extractConnectionStrings(file, keyFilter)
		if err != nil {
			// Skip files that fail to parse
			continue
		}
		found = append(found, conns...)
	}

	return found, nil
}

func extractConnectionStrings(filePath, keyFilter string) ([]FoundConnection, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var config map[string]any
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	var found []FoundConnection

	// Search in "ConnectionStrings" section
	if cs, ok := config["ConnectionStrings"]; ok {
		if csMap, ok := cs.(map[string]any); ok {
			for k, v := range csMap {
				if s, ok := v.(string); ok {
					if keyFilter != "" && !strings.EqualFold(k, keyFilter) {
						continue
					}
					if looksLikeMSSQLConnStr(s) {
						found = append(found, FoundConnection{
							File: filePath,
							Key:  "ConnectionStrings:" + k,
							DSN:  s,
						})
					}
				}
			}
		}
	}

	// Also search all top-level string values that look like connection strings
	// This catches patterns like "umbracoDbDSN" at the root level
	for k, v := range config {
		if k == "ConnectionStrings" {
			continue
		}
		if s, ok := v.(string); ok {
			if keyFilter != "" && !strings.EqualFold(k, keyFilter) {
				continue
			}
			if looksLikeMSSQLConnStr(s) {
				found = append(found, FoundConnection{
					File: filePath,
					Key:  k,
					DSN:  s,
				})
			}
		}
	}

	return found, nil
}

func looksLikeMSSQLConnStr(s string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, "server=") || strings.Contains(lower, "data source=")
}

// resolveConnectionString returns an explicit connection string, or discovers one from appsettings.
func resolveConnectionString(connStr, dir, key string) (string, error) {
	if connStr != "" {
		return connStr, nil
	}

	conns, err := discoverConnectionStrings(dir, key)
	if err != nil {
		return "", fmt.Errorf("discovering connection strings: %w", err)
	}
	if len(conns) == 0 {
		return "", fmt.Errorf("no MSSQL connection strings found in appsettings.json files (searched dir: %s)", dir)
	}

	return conns[0].DSN, nil
}

// toGoMSSQLConnStr converts an ADO.NET-style connection string to a Go sqlserver:// URL.
func toGoMSSQLConnStr(connStr string) string {
	// If it already looks like a URL, use as-is
	if strings.HasPrefix(connStr, "sqlserver://") {
		return connStr
	}

	parts := parseConnStrParts(connStr)

	server := parts["server"]
	database := parts["database"]
	userID := parts["user id"]
	password := parts["password"]
	host := server
	port := "1433"

	// Handle "Server=host,port" format
	if idx := strings.Index(server, ","); idx != -1 {
		host = server[:idx]
		port = server[idx+1:]
	}

	// Build URL
	url := fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s",
		userID, password, host, port, database)

	// Pass through extra params
	skip := map[string]bool{
		"server": true, "database": true, "user id": true, "password": true,
		"initial catalog": true, "data source": true,
	}
	for k, v := range parts {
		if skip[k] {
			continue
		}
		url += "&" + k + "=" + v
	}

	return url
}

func parseConnStrParts(connStr string) map[string]string {
	parts := make(map[string]string)
	for _, segment := range strings.Split(connStr, ";") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		idx := strings.Index(segment, "=")
		if idx == -1 {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(segment[:idx]))
		val := strings.TrimSpace(segment[idx+1:])

		// Normalize aliases
		switch key {
		case "data source":
			key = "server"
		case "initial catalog":
			key = "database"
		case "uid":
			key = "user id"
		case "pwd":
			key = "password"
		}

		parts[key] = val
	}
	return parts
}

func openDB(connStr string) (*sql.DB, error) {
	goConnStr := toGoMSSQLConnStr(connStr)
	db, err := sql.Open("sqlserver", goConnStr)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("connecting to database: %w", err)
	}
	return db, nil
}

// ---------- Tool implementations ----------

func executeDiscover(dir, key string) (*DiscoverResult, error) {
	conns, err := discoverConnectionStrings(dir, key)
	if err != nil {
		return nil, err
	}
	return &DiscoverResult{
		Connections: conns,
		Count:       len(conns),
	}, nil
}

func executeQuery(connStr, query, dir, key string) (*QueryResult, error) {
	resolved, err := resolveConnectionString(connStr, dir, key)
	if err != nil {
		return nil, err
	}

	db, err := openDB(resolved)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("executing query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("getting columns: %w", err)
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("scanning row: %w", err)
		}

		row := make(map[string]any)
		for i, col := range columns {
			val := values[i]
			// Convert []byte to string for JSON serialization
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating rows: %w", err)
	}

	return &QueryResult{
		Columns:  columns,
		Rows:     results,
		RowCount: len(results),
	}, nil
}

func executeTables(connStr, dir, key string) (*TablesResult, error) {
	resolved, err := resolveConnectionString(connStr, dir, key)
	if err != nil {
		return nil, err
	}

	db, err := openDB(resolved)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	query := `SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
		FROM INFORMATION_SCHEMA.TABLES
		ORDER BY TABLE_SCHEMA, TABLE_NAME`

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("listing tables: %w", err)
	}
	defer rows.Close()

	var tables []TableInfo
	for rows.Next() {
		var t TableInfo
		if err := rows.Scan(&t.Schema, &t.Name, &t.Type); err != nil {
			return nil, fmt.Errorf("scanning table: %w", err)
		}
		tables = append(tables, t)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating tables: %w", err)
	}

	return &TablesResult{
		Tables: tables,
		Count:  len(tables),
	}, nil
}

func executeSchema(connStr, table, dir, key string) (*SchemaResult, error) {
	resolved, err := resolveConnectionString(connStr, dir, key)
	if err != nil {
		return nil, err
	}

	db, err := openDB(resolved)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Split schema.table
	schemaName := "dbo"
	tableName := table
	if idx := strings.Index(table, "."); idx != -1 {
		schemaName = table[:idx]
		tableName = table[idx+1:]
	}

	query := `SELECT
		c.COLUMN_NAME,
		c.DATA_TYPE,
		c.CHARACTER_MAXIMUM_LENGTH,
		c.IS_NULLABLE,
		c.COLUMN_DEFAULT,
		CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS IS_PK
	FROM INFORMATION_SCHEMA.COLUMNS c
	LEFT JOIN (
		SELECT ku.COLUMN_NAME
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
			ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
		WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
			AND tc.TABLE_SCHEMA = @schema
			AND tc.TABLE_NAME = @table
	) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
	WHERE c.TABLE_SCHEMA = @schema AND c.TABLE_NAME = @table
	ORDER BY c.ORDINAL_POSITION`

	rows, err := db.Query(query, sql.Named("schema", schemaName), sql.Named("table", tableName))
	if err != nil {
		return nil, fmt.Errorf("querying schema: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var maxLen sql.NullInt64
		var nullable string
		var defaultVal sql.NullString
		var isPK int

		if err := rows.Scan(&col.Name, &col.DataType, &maxLen, &nullable, &defaultVal, &isPK); err != nil {
			return nil, fmt.Errorf("scanning column: %w", err)
		}

		if maxLen.Valid {
			col.MaxLength = maxLen.Int64
		}
		col.IsNullable = strings.EqualFold(nullable, "YES")
		if defaultVal.Valid {
			col.Default = defaultVal.String
		}
		col.IsPK = isPK == 1

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating columns: %w", err)
	}

	return &SchemaResult{
		Table:   table,
		Columns: columns,
	}, nil
}
