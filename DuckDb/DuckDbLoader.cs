using DuckDB.NET.Data;
using BakShell.Mdf;

namespace BakShell.DuckDb;

public class DuckDbLoader : IDisposable
{
    private readonly DuckDBConnection _conn;

    public DuckDbLoader(string path)
    {
        if (File.Exists(path))
            File.Delete(path);

        _conn = new DuckDBConnection($"Data Source={path}");
        _conn.Open();
    }

    public void CreateTable(string name, IReadOnlyList<ColumnType> columns)
    {
        var colDefs = columns
            .Where(c => !c.Computed)
            .Select(c =>
            {
                string quotedName = $"\"{c.Name.Replace("\"", "\"\"")}\"";
                string duckType = TypeMapping.SqlTypeToDuckDb(c.DataType);
                return $"{quotedName} {duckType}";
            });

        string quotedTable = $"\"{name.Replace("\"", "\"\"")}\"";
        string ddl = $"CREATE TABLE {quotedTable} ({string.Join(", ", colDefs)})";

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
    }

    public long LoadTable(MdfTable table, IPageProvider pageProvider)
    {
        var nonComputed = table.Schema.Columns.Where(c => !c.Computed).ToList();
        if (nonComputed.Count == 0)
        {
            Console.WriteLine($"  {table.Name}: skipped (no columns)");
            return 0;
        }

        CreateTable(table.Name, nonComputed);

        // Build column indices for non-computed columns
        var nonComputedIndices = new List<int>();
        for (int i = 0; i < table.Schema.Columns.Count; i++)
        {
            if (!table.Schema.Columns[i].Computed)
                nonComputedIndices.Add(i);
        }

        int colCount = nonComputedIndices.Count;
        long rowCount = 0;
        long errorCount = 0;

        // Use Appender API for fast bulk loading
        try
        {
            using var appender = _conn.CreateAppender(table.Name);

            foreach (var row in table.Rows())
            {
                try
                {
                    var appRow = appender.CreateRow();

                    for (int i = 0; i < colCount; i++)
                    {
                        int srcIdx = nonComputedIndices[i];
                        SqlValue? val = srcIdx < row.Length ? row[srcIdx] : null;
                        object? duckVal = TypeMapping.ConvertToDuckDbValue(val, pageProvider);
                        AppendValueToRow(appRow, duckVal, nonComputed[i].DataType);
                    }

                    appRow.EndRow();
                    rowCount++;
                }
                catch (Exception e)
                {
                    errorCount++;
                    if (errorCount <= 3)
                        Console.Error.WriteLine($"    Row error in {table.Name}: {e.Message}");
                }

                if (rowCount % 100_000 == 0 && rowCount > 0)
                    Console.Write($"\r  {table.Name}: {rowCount:N0} rows...");
            }
        }
        catch (Exception)
        {
            // Fall back to parameterized INSERT if Appender fails
            return LoadTableFallback(table, pageProvider, nonComputed, nonComputedIndices);
        }

        if (errorCount > 0)
            Console.WriteLine($"\r  {table.Name}: {rowCount:N0} rows loaded ({errorCount:N0} errors)          ");
        else
            Console.WriteLine($"\r  {table.Name}: {rowCount:N0} rows loaded          ");

        return rowCount;
    }

    private static void AppendValueToRow(IDuckDBAppenderRow row, object? value, SqlType colType)
    {
        if (value == null || value == DBNull.Value)
        {
            row.AppendNullValue();
            return;
        }

        switch (value)
        {
            case bool b:
                row.AppendValue(b);
                break;
            case sbyte sb:
                row.AppendValue(sb);
                break;
            case short s:
                row.AppendValue(s);
                break;
            case int i:
                row.AppendValue(i);
                break;
            case long l:
                row.AppendValue(l);
                break;
            case double d:
                row.AppendValue(d);
                break;
            case DateTime dt:
                row.AppendValue(dt);
                break;
            case string str:
                row.AppendValue(str);
                break;
            case byte[] blob:
                row.AppendValue(blob);
                break;
            default:
                row.AppendValue(value.ToString());
                break;
        }
    }

    private long LoadTableFallback(MdfTable table, IPageProvider pageProvider,
        List<ColumnType> nonComputed, List<int> nonComputedIndices)
    {
        int colCount = nonComputedIndices.Count;

        // Build parameterized INSERT
        string quotedTable = $"\"{table.Name.Replace("\"", "\"\"")}\"";
        var paramNames = Enumerable.Range(0, colCount).Select(i => $"$p{i}");
        string insertSql = $"INSERT INTO {quotedTable} VALUES ({string.Join(", ", paramNames)})";

        using var cmd = _conn.CreateCommand();
        cmd.CommandText = insertSql;
        for (int i = 0; i < colCount; i++)
        {
            cmd.Parameters.Add(new DuckDBParameter($"p{i}", null));
        }

        long rowCount = 0;
        long errorCount = 0;

        using var txn = _conn.BeginTransaction();

        foreach (var row in table.Rows())
        {
            try
            {
                for (int i = 0; i < colCount; i++)
                {
                    int srcIdx = nonComputedIndices[i];
                    SqlValue? val = srcIdx < row.Length ? row[srcIdx] : null;
                    object? duckVal = TypeMapping.ConvertToDuckDbValue(val, pageProvider);
                    cmd.Parameters[i].Value = duckVal ?? DBNull.Value;
                }

                cmd.ExecuteNonQuery();
                rowCount++;
            }
            catch (Exception e)
            {
                errorCount++;
                if (errorCount <= 3)
                    Console.Error.WriteLine($"    Row error in {table.Name}: {e.Message}");
            }

            if (rowCount % 100_000 == 0 && rowCount > 0)
                Console.Write($"\r  {table.Name}: {rowCount:N0} rows...");
        }

        txn.Commit();

        if (errorCount > 0)
            Console.WriteLine($"\r  {table.Name}: {rowCount:N0} rows loaded ({errorCount:N0} errors)          ");
        else
            Console.WriteLine($"\r  {table.Name}: {rowCount:N0} rows loaded          ");

        return rowCount;
    }

    public void RunQuery(string sql)
    {
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        using var reader = cmd.ExecuteReader();

        int colCount = reader.FieldCount;

        // Print header
        var headers = new string[colCount];
        for (int i = 0; i < colCount; i++)
            headers[i] = reader.GetName(i);

        Console.WriteLine(string.Join(" | ", headers.Select(h => h.PadRight(20))));
        Console.WriteLine(string.Join("-+-", headers.Select(_ => new string('-', 20))));

        int count = 0;
        while (reader.Read())
        {
            var values = new string[colCount];
            for (int i = 0; i < colCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? "NULL" : reader.GetValue(i)?.ToString() ?? "NULL";
            }
            Console.WriteLine(string.Join(" | ", values.Select(v => v.PadRight(20))));
            count++;
        }

        Console.WriteLine($"\n({count} rows)");
    }

    // REPL-friendly methods

    public bool TableExists(string tableName)
    {
        try
        {
            var quotedName = tableName.Replace("\"", "\"\"");
            var sql = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{quotedName.ToLower()}'";
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = sql;
            var result = cmd.ExecuteScalar();
            return result != null && Convert.ToInt64(result) > 0;
        }
        catch
        {
            return false;
        }
    }

    public Repl.QueryResult ExecuteQueryWithResults(string sql)
    {
        try
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = sql;
            using var reader = cmd.ExecuteReader();

            // Build result structure
            var columns = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++)
                columns.Add(reader.GetName(i));

            var rows = new List<object?[]>();
            while (reader.Read())
            {
                var row = new object?[reader.FieldCount];
                reader.GetValues(row);
                rows.Add(row);
            }

            return new Repl.QueryResult(columns, rows);
        }
        catch (Exception ex)
        {
            return Repl.QueryResult.Error(ex.Message);
        }
    }

    public long GetRowCount(string tableName)
    {
        var quotedName = $"\"{tableName.Replace("\"", "\"\"")}\"";
        var sql = $"SELECT COUNT(*) FROM {quotedName}";
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    public void Dispose()
    {
        _conn.Dispose();
    }
}
