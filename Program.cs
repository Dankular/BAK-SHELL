using System.Diagnostics;
using System.Text;
using BakShell.DuckDb;
using BakShell.Mdf;
using BakShell.Mtf;
using BakShell.Repl;

// Register Windows-1252 encoding
Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

// Parse command-line arguments
string? backupPath = null;
string? outputPath = null;
string? duckDbPath = null;
List<string>? tableFilter = null;
bool listOnly = false;
bool interactive = false;
string? query = null;

for (int i = 0; i < args.Length; i++)
{
    switch (args[i])
    {
        case "-i" or "--interactive":
            interactive = true;
            break;
        case "--db":
            if (i + 1 < args.Length) duckDbPath = args[++i];
            break;
        case "-o" or "--output":
            if (i + 1 < args.Length) outputPath = args[++i];
            break;
        case "-t" or "--table":
            if (i + 1 < args.Length)
                tableFilter = args[++i].Split(',', StringSplitOptions.RemoveEmptyEntries).ToList();
            break;
        case "--list":
            listOnly = true;
            break;
        case "-q" or "--query":
            if (i + 1 < args.Length) query = args[++i];
            break;
        case "-h" or "--help":
            PrintUsage();
            return 0;
        default:
            if (!args[i].StartsWith('-'))
                backupPath = args[i];
            break;
    }
}

if (backupPath == null)
{
    PrintUsage();
    return 1;
}

if (!File.Exists(backupPath))
{
    Console.Error.WriteLine($"Backup file not found: {backupPath}");
    return 1;
}

// Interactive REPL mode
if (interactive)
{
    duckDbPath ??= ":memory:";

    Console.WriteLine("=== BakShell Interactive Shell ===\n");

    using var session = new ReplSession(backupPath, duckDbPath);
    var executor = new CommandExecutor(session);

    Console.WriteLine("Type 'help' for commands, 'exit' to quit.\n");

    while (true)
    {
        Console.Write("bak2duckdb> ");
        var input = Console.ReadLine();

        if (string.IsNullOrWhiteSpace(input))
            continue;

        try
        {
            var cmd = CommandParser.Parse(input);

            if (cmd.Type == CommandType.BuiltIn)
                executor.Execute(cmd);
            else
                executor.ExecuteQuery(cmd.Command);
        }
        catch (Exception ex)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Error: {ex.Message}");
            Console.ResetColor();
        }

        Console.WriteLine();
    }
}

// Batch mode (existing behavior)
outputPath ??= Path.ChangeExtension(backupPath, ".duckdb");

Console.WriteLine("=== BakShell (C#) ===");
Console.WriteLine($"Backup: {backupPath}");
Console.WriteLine();

// Warn about UNC paths
if (backupPath.StartsWith(@"\\") || backupPath.StartsWith("//"))
{
    Console.WriteLine("NOTE: Backup is on a network share. For best performance, copy to a local drive.");
    Console.WriteLine();
}

var sw = Stopwatch.StartNew();

// Step 1: Parse MTF to find MQDA stream
Console.WriteLine("Parsing backup file...");
using var parser = new MtfParser(backupPath);

var mqda = parser.FindMqdaStream();
if (mqda == null)
{
    Console.Error.WriteLine("ERROR: No database stream (MQDA) found in backup file.");
    Console.Error.WriteLine("Is this a valid SQL Server .BAK file?");
    return 1;
}

Console.WriteLine($"  Found MQDA stream ({mqda.Value.length:N0} bytes)");

// Step 2: Build page index
Console.WriteLine("  Building page index...");
var pageProvider = new MtfPageProvider(backupPath, mqda.Value.offset, mqda.Value.length);

// Step 3: Parse MDF system tables
Console.WriteLine("  Parsing database catalog...");
var db = new MdfDatabase(pageProvider);

var allTables = db.AllTables().ToList();
var userTables = db.UserTables().ToList();
Console.WriteLine($"\nFound {allTables.Count} tables ({userTables.Count} user tables).\n");

// List mode
if (listOnly)
{
    Console.WriteLine($"{"Type",-15} {"Table",-40} {"Columns",10}");
    Console.WriteLine($"{new string('-', 15)} {new string('-', 40)} {new string('-', 10)}");
    foreach (var tbl in allTables)
    {
        string typeName = tbl.Type == SchType.UserTable ? "UserTable" : "SystemTable";
        int colCount = db.ColumnsForTable(tbl).Count();
        Console.WriteLine($"{typeName,-15} {tbl.Name,-40} {colCount,10}");
    }
    Console.WriteLine("\nUse --table <name> to extract specific tables.");
    return 0;
}

// Filter tables
var tablesToExtract = userTables
    .Where(t => tableFilter == null || tableFilter.Any(f => f.Equals(t.Name, StringComparison.OrdinalIgnoreCase)))
    .ToList();

if (tablesToExtract.Count == 0)
{
    Console.WriteLine("No matching tables to extract.");
    return 0;
}

Console.WriteLine($"Extracting {tablesToExtract.Count} tables to: {outputPath}\n");

// Step 4: Load into DuckDB
using var loader = new DuckDbLoader(outputPath);
long totalRows = 0;

foreach (var tableObj in tablesToExtract)
{
    try
    {
        var table = db.BuildTable(tableObj);
        long rowCount = loader.LoadTable(table, pageProvider);
        totalRows += rowCount;
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"  WARNING: Failed to load table '{tableObj.Name}': {ex.Message}");
    }
}

sw.Stop();
Console.WriteLine($"\n=== Summary ===");
Console.WriteLine($"Tables loaded: {tablesToExtract.Count}");
Console.WriteLine($"Total rows:    {totalRows:N0}");
Console.WriteLine($"Output:        {outputPath}");
Console.WriteLine($"Elapsed:       {sw.Elapsed.TotalSeconds:F1}s");

// Run optional query
if (query != null)
{
    Console.WriteLine("\n=== Query Results ===");
    loader.RunQuery(query);
}

return 0;

static void PrintUsage()
{
    Console.WriteLine("Usage: BakShell <backup.bak> [options]");
    Console.WriteLine();
    Console.WriteLine("Modes:");
    Console.WriteLine("  -i, --interactive     Launch interactive REPL shell (explore backup interactively)");
    Console.WriteLine("  (default)             Batch extraction mode (extract all tables to DuckDB)");
    Console.WriteLine();
    Console.WriteLine("Interactive Mode Options:");
    Console.WriteLine("  --db <file>           DuckDB file for session (default: in-memory)");
    Console.WriteLine();
    Console.WriteLine("Batch Mode Options:");
    Console.WriteLine("  -o, --output <file>   Output DuckDB file (default: <backup>.duckdb)");
    Console.WriteLine("  -t, --table <names>   Only extract specific table(s), comma-separated");
    Console.WriteLine("  --list                List tables without extracting");
    Console.WriteLine("  -q, --query <sql>     Run SQL query after loading");
    Console.WriteLine();
    Console.WriteLine("General Options:");
    Console.WriteLine("  -h, --help            Show this help");
    Console.WriteLine();
    Console.WriteLine("Examples:");
    Console.WriteLine("  BakShell backup.bak -i                    # Interactive mode");
    Console.WriteLine("  BakShell backup.bak -i --db data.duckdb  # Interactive with persistent DB");
    Console.WriteLine("  BakShell backup.bak --list                # List tables");
    Console.WriteLine("  BakShell backup.bak -t Company,Movement  # Extract specific tables");
    Console.WriteLine("  BakShell backup.bak -o output.duckdb     # Extract all tables");
}
