namespace BakShell.Repl;

public static class TableFormatter
{
    public static void DisplayTable(QueryResult result, int maxColumnWidth = 50)
    {
        if (result.Rows.Count == 0)
        {
            Console.WriteLine("(No rows returned)");
            return;
        }

        // Calculate column widths
        var columnWidths = new int[result.Columns.Count];
        for (int i = 0; i < result.Columns.Count; i++)
        {
            // Start with header width
            columnWidths[i] = result.Columns[i].Length;

            // Check all row values
            foreach (var row in result.Rows)
            {
                var valueStr = FormatValue(row[i]);
                columnWidths[i] = Math.Max(columnWidths[i], valueStr.Length);
            }

            // Cap at max width
            columnWidths[i] = Math.Min(columnWidths[i], maxColumnWidth);
        }

        // Print header
        var headerParts = new List<string>();
        for (int i = 0; i < result.Columns.Count; i++)
        {
            headerParts.Add(result.Columns[i].PadRight(columnWidths[i]));
        }
        Console.WriteLine(string.Join(" | ", headerParts));

        // Print separator
        var separatorParts = new List<string>();
        for (int i = 0; i < result.Columns.Count; i++)
        {
            separatorParts.Add(new string('-', columnWidths[i]));
        }
        Console.WriteLine(string.Join("-+-", separatorParts));

        // Print rows
        foreach (var row in result.Rows)
        {
            var rowParts = new List<string>();
            for (int i = 0; i < result.Columns.Count; i++)
            {
                var valueStr = FormatValue(row[i]);
                if (valueStr.Length > columnWidths[i])
                    valueStr = valueStr.Substring(0, columnWidths[i] - 3) + "...";

                rowParts.Add(valueStr.PadRight(columnWidths[i]));
            }
            Console.WriteLine(string.Join(" | ", rowParts));
        }
    }

    private static string FormatValue(object? value)
    {
        if (value == null || value is DBNull)
            return "NULL";

        if (value is DateTime dt)
            return dt.ToString("yyyy-MM-dd HH:mm:ss");

        if (value is byte[] bytes)
            return $"<{bytes.Length} bytes>";

        return value.ToString() ?? "NULL";
    }
}
