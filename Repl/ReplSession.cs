using BakShell.Mdf;
using BakShell.Mtf;
using BakShell.DuckDb;

namespace BakShell.Repl;

public class ReplSession : IDisposable
{
    private readonly MtfParser _parser;
    private readonly MtfPageProvider _pageProvider;
    private readonly DuckDbLoader _loader;

    public MdfDatabase Database { get; }
    public string BackupPath { get; }

    public ReplSession(string backupPath, string duckDbPath)
    {
        BackupPath = backupPath;

        Console.WriteLine("Initializing session...");
        Console.Write("  Parsing backup file...");

        _parser = new MtfParser(backupPath);
        var mqda = _parser.FindMqdaStream()
            ?? throw new InvalidDataException("No MQDA stream found");

        Console.WriteLine($" Found MQDA stream ({mqda.length:N0} bytes)");
        Console.Write("  Building page index...");

        _pageProvider = new MtfPageProvider(backupPath, mqda.offset, mqda.length);

        Console.WriteLine(" Done");
        Console.Write("  Parsing database catalog...");

        Database = new MdfDatabase(_pageProvider);

        Console.WriteLine(" Done");
        Console.WriteLine($"\nReady! Database: {Database.Boot.DatabaseName}");
        Console.WriteLine($"Found {Database.UserTables().Count()} user tables.\n");

        _loader = new DuckDbLoader(duckDbPath);
    }

    public IEnumerable<SysSchObj> ListTables()
    {
        return Database.UserTables();
    }

    public IEnumerable<(string name, string type, bool nullable)> GetTableSchema(string tableName)
    {
        var table = Database.SchObjs.FirstOrDefault(t =>
            t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));

        if (table == null)
            throw new InvalidOperationException($"Table '{tableName}' not found");

        return Database.ColumnsForTable(table).Select(col =>
        {
            var scalarType = Database.TypeForColumn(col);
            var sqlType = scalarType != null ? SqlType.FromColumn(col, scalarType) : null;
            var duckType = sqlType != null ? TypeMapping.SqlTypeToDuckDb(sqlType) : "UNKNOWN";

            return (col.Name ?? "?", duckType, !col.Status.HasFlag(ColParStatus.Nullable));
        });
    }

    public QueryResult ExecuteQuery(string sql)
    {
        return _loader.ExecuteQueryWithResults(sql);
    }

    public bool IsTableLoaded(string tableName)
    {
        return _loader.TableExists(tableName);
    }

    public void EnsureTableLoaded(string tableName)
    {
        if (IsTableLoaded(tableName))
            return;

        var tableObj = Database.SchObjs.FirstOrDefault(t =>
            t.Name.Equals(tableName, StringComparison.OrdinalIgnoreCase));

        if (tableObj == null)
            throw new InvalidOperationException($"Table '{tableName}' not found in backup");

        Console.Write($"Loading table '{tableName}' from backup...");

        var table = Database.BuildTable(tableObj);
        long rowCount = _loader.LoadTable(table, _pageProvider);

        Console.WriteLine($" {rowCount:N0} rows loaded");
    }

    public void LoadAllTables()
    {
        var tables = Database.UserTables().ToList();
        Console.WriteLine($"Loading {tables.Count} tables from backup...\n");

        long totalRows = 0;
        foreach (var tableObj in tables)
        {
            try
            {
                var table = Database.BuildTable(tableObj);
                long rowCount = _loader.LoadTable(table, _pageProvider);
                totalRows += rowCount;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"  WARNING: Failed to load table '{tableObj.Name}': {ex.Message}");
            }
        }

        Console.WriteLine($"\nTotal: {totalRows:N0} rows loaded across {tables.Count} tables");
    }

    public void Dispose()
    {
        _loader?.Dispose();
        _pageProvider?.Dispose();
        _parser?.Dispose();
    }
}
