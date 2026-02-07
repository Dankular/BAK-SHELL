namespace BakShell.Repl;

public class QueryResult
{
    public bool IsError { get; init; }
    public string? ErrorMessage { get; init; }
    public List<string> Columns { get; init; }
    public List<object?[]> Rows { get; init; }
    public int RowCount => Rows.Count;

    public QueryResult(List<string> columns, List<object?[]> rows)
    {
        IsError = false;
        Columns = columns;
        Rows = rows;
    }

    private QueryResult(string errorMessage)
    {
        IsError = true;
        ErrorMessage = errorMessage;
        Columns = new List<string>();
        Rows = new List<object?[]>();
    }

    public static QueryResult Error(string message) => new(message);
}
