namespace BakShell.Mdf;

public class ColumnType
{
    public int Idx { get; init; }
    public SqlType DataType { get; init; } = null!;
    public string Name { get; init; } = "";
    public bool Nullable { get; init; }
    public bool Computed { get; init; }
}

public class Schema
{
    public List<ColumnType> Columns { get; }

    public Schema(List<ColumnType> columns)
    {
        columns.Sort((a, b) => a.Idx.CompareTo(b.Idx));
        Columns = columns;
    }

    public SqlValue?[] ParseRow(Record record)
    {
        var values = new SqlValue?[Columns.Count];
        int fixedCursor = 0;
        var bitParser = new BitParser();
        ushort varColumnIdx = 0;
        int nullBitIdx = 0;

        for (int i = 0; i < Columns.Count; i++)
        {
            var col = Columns[i];

            if (col.Computed)
                continue;

            if (nullBitIdx >= record.ColumnCount)
            {
                // Past the record's column count — treat as null
            }
            else if (!record.IsColumnNull(nullBitIdx))
            {
                if (col.DataType.IsVarLength)
                {
                    if (record.VarColumns != null)
                    {
                        var (complex, data) = record.VarColumns.Get(varColumnIdx);
                        try
                        {
                            values[i] = SqlTypeParser.ParseVarLength(col.DataType, complex, data);
                        }
                        catch
                        {
                            // Graceful degradation - skip broken values
                        }
                        varColumnIdx++;
                    }
                    else
                    {
                        try
                        {
                            values[i] = SqlTypeParser.ParseVarLength(col.DataType, false, ReadOnlyMemory<byte>.Empty);
                        }
                        catch { }
                    }
                }
                else
                {
                    try
                    {
                        values[i] = SqlTypeParser.ParseFixed(col.DataType, bitParser, record.FixedData.Span, ref fixedCursor);
                    }
                    catch
                    {
                        // Graceful degradation
                    }
                }
            }

            nullBitIdx++;
        }

        return values;
    }
}
