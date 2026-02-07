using System.Diagnostics;

namespace BakShell.Repl;

public class CommandExecutor
{
    private readonly ReplSession _session;

    public CommandExecutor(ReplSession session)
    {
        _session = session;
    }

    public void Execute(ParsedCommand cmd)
    {
        switch (cmd.Command)
        {
            case "help":
                ShowHelp();
                break;

            case "exit":
                Console.WriteLine("Goodbye!");
                Environment.Exit(0);
                break;

            case "show_tables":
                ShowTables();
                break;

            case "describe":
                DescribeTable(cmd.Args["table"]);
                break;

            case "preview":
                PreviewTable(cmd.Args["table"],
                    cmd.Args.GetValueOrDefault("limit", "10"));
                break;

            case "load":
                LoadTable(cmd.Args["table"]);
                break;

            case "load_all":
                LoadAllTables();
                break;

            default:
                throw new InvalidOperationException($"Unknown command: {cmd.Command}");
        }
    }

    public void ExecuteQuery(string sql)
    {
        var sw = Stopwatch.StartNew();
        var result = _session.ExecuteQuery(sql);
        sw.Stop();

        if (result.IsError)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {result.ErrorMessage}");
            Console.ResetColor();
            return;
        }

        if (result.RowCount == 0)
        {
            Console.WriteLine($"(Query executed successfully, no rows returned)");
            Console.WriteLine($"Time: {sw.ElapsedMilliseconds}ms");
            return;
        }

        TableFormatter.DisplayTable(result);
        Console.WriteLine($"\n({result.RowCount:N0} rows in {sw.ElapsedMilliseconds}ms)");
    }

    private void ShowHelp()
    {
        Console.WriteLine(@"
BakShell Interactive Shell - Available Commands:

Built-in Commands:
  help, ?              Show this help message
  show tables, tables  List all tables in backup
  describe <table>     Show schema for table
  preview <table> [N]  Show first N rows (default 10)
  load <table>         Load table from backup into DuckDB
  load all             Load all user tables into DuckDB
  exit, quit, q        Exit interactive mode

SQL Queries:
  Run any DuckDB SQL query against loaded tables.
  Tables are auto-loaded on first query.

Export Examples:
  COPY Company TO 'company.csv' (HEADER, DELIMITER ',');
  COPY Movement TO 'movement.parquet' (FORMAT PARQUET);
  COPY Goods TO 'goods.json' (FORMAT JSON);

Database Connection Examples (requires extensions):
  INSTALL postgres_scanner;
  LOAD postgres_scanner;
  ATTACH 'dbname=mydb user=postgres host=localhost' AS pg (TYPE POSTGRES);
  CREATE TABLE pg.company AS SELECT * FROM Company;

Query Examples:
  SELECT COUNT(*) FROM Company;
  SELECT * FROM Movement WHERE CreatedDate >= '2023-01-01';
  SELECT c.CompanyName, COUNT(*) FROM Company c JOIN Movement m ON c.CompanyId = m.CompanyId GROUP BY c.CompanyName;
");
    }

    private void ShowTables()
    {
        var tables = _session.ListTables().ToList();

        Console.WriteLine($"\nTables in backup ({tables.Count} total):\n");
        Console.WriteLine($"{"Name",-50} {"Loaded",-10}");
        Console.WriteLine($"{new string('-', 50)} {new string('-', 10)}");

        foreach (var table in tables)
        {
            var loaded = _session.IsTableLoaded(table.Name) ? "Yes" : "No";
            Console.WriteLine($"{table.Name,-50} {loaded,-10}");
        }

        Console.WriteLine($"\nUse 'load <table>' to load into DuckDB, or 'load all' for all tables.");
        Console.WriteLine($"Use 'describe <table>' to view schema.");
    }

    private void DescribeTable(string tableName)
    {
        try
        {
            var schema = _session.GetTableSchema(tableName).ToList();

            Console.WriteLine($"\nTable: {tableName}");
            Console.WriteLine($"Database: {_session.Database.Boot.DatabaseName}\n");

            Console.WriteLine($"{"Column",-30} {"Type",-20} {"Nullable",-10}");
            Console.WriteLine($"{new string('-', 30)} {new string('-', 20)} {new string('-', 10)}");

            foreach (var (name, type, nullable) in schema)
            {
                var nullableStr = nullable ? "No" : "Yes";
                Console.WriteLine($"{name,-30} {type,-20} {nullableStr,-10}");
            }

            Console.WriteLine($"\n({schema.Count} columns)");

            if (!_session.IsTableLoaded(tableName))
                Console.WriteLine($"\nNote: Table not yet loaded. Use 'load {tableName}' or query it to auto-load.");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();
        }
    }

    private void PreviewTable(string tableName, string limitStr)
    {
        try
        {
            if (!int.TryParse(limitStr, out int limit) || limit < 1)
                limit = 10;

            // Auto-load if needed
            if (!_session.IsTableLoaded(tableName))
            {
                Console.WriteLine($"Table '{tableName}' not loaded yet, loading from backup...");
                _session.EnsureTableLoaded(tableName);
                Console.WriteLine();
            }

            var sql = $"SELECT * FROM \"{tableName.Replace("\"", "\"\"")}\" LIMIT {limit}";
            var sw = Stopwatch.StartNew();
            var result = _session.ExecuteQuery(sql);
            sw.Stop();

            if (result.IsError)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"Error: {result.ErrorMessage}");
                Console.ResetColor();
                return;
            }

            Console.WriteLine($"\nPreviewing {tableName} (first {limit} rows):\n");
            TableFormatter.DisplayTable(result);
            Console.WriteLine($"\n(Showing {result.RowCount} rows in {sw.ElapsedMilliseconds}ms)");
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();
        }
    }

    private void LoadTable(string tableName)
    {
        try
        {
            if (_session.IsTableLoaded(tableName))
            {
                Console.WriteLine($"Table '{tableName}' is already loaded.");
                return;
            }

            _session.EnsureTableLoaded(tableName);
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();
        }
    }

    private void LoadAllTables()
    {
        try
        {
            _session.LoadAllTables();
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();
        }
    }
}
