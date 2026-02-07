namespace BakShell.Repl;

public enum CommandType { BuiltIn, SqlQuery }

public record ParsedCommand(
    CommandType Type,
    string Command,
    Dictionary<string, string> Args
);

public static class CommandParser
{
    public static ParsedCommand Parse(string input)
    {
        var trimmed = input.Trim();
        var lower = trimmed.ToLower();

        // Check for built-in commands
        if (lower is "help" or "?")
            return new ParsedCommand(CommandType.BuiltIn, "help", new());

        if (lower is "exit" or "quit" or "q")
            return new ParsedCommand(CommandType.BuiltIn, "exit", new());

        if (lower is "show tables" or "tables" or "\\dt")
            return new ParsedCommand(CommandType.BuiltIn, "show_tables", new());

        if (lower.StartsWith("describe ") || lower.StartsWith("desc "))
        {
            var parts = trimmed.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2)
                throw new ArgumentException("DESCRIBE requires a table name");

            return new ParsedCommand(CommandType.BuiltIn, "describe",
                new() { ["table"] = parts[1] });
        }

        if (lower.StartsWith("preview "))
        {
            var parts = trimmed.Split(' ', 3, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2)
                throw new ArgumentException("PREVIEW requires a table name");

            var args = new Dictionary<string, string> { ["table"] = parts[1] };
            if (parts.Length == 3 && int.TryParse(parts[2], out int limit))
                args["limit"] = limit.ToString();

            return new ParsedCommand(CommandType.BuiltIn, "preview", args);
        }

        if (lower is "load all")
            return new ParsedCommand(CommandType.BuiltIn, "load_all", new());

        if (lower.StartsWith("load "))
        {
            var parts = trimmed.Split(' ', 2, StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2)
                throw new ArgumentException("LOAD requires a table name");

            return new ParsedCommand(CommandType.BuiltIn, "load",
                new() { ["table"] = parts[1] });
        }

        // Everything else is a SQL query
        return new ParsedCommand(CommandType.SqlQuery, trimmed, new());
    }
}
