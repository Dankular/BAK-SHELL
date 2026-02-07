using System.Buffers.Binary;
using System.Text;

namespace BakShell.Mdf;

public enum SqlTypeKind
{
    TinyInt, SmallInt, Int, BigInt,
    Binary, Char, NChar,
    VarBinary, VarChar,
    Bit, SqlVariant,
    NVarChar, SysName,
    DateTime, SmallDateTime,
    UniqueIdentifier,
    Image, NText, Float,
}

public class SqlType
{
    public SqlTypeKind Kind { get; }
    public int Size { get; } // For Binary, Char, NChar, etc.

    public SqlType(SqlTypeKind kind, int size = 0)
    {
        Kind = kind;
        Size = size;
    }

    public static SqlType FromColumn(SysColPar col, SysScalarType ty)
    {
        return ty.Name.ToLowerInvariant() switch
        {
            "tinyint" => new SqlType(SqlTypeKind.TinyInt),
            "smallint" => new SqlType(SqlTypeKind.SmallInt),
            "int" => new SqlType(SqlTypeKind.Int),
            "bigint" => new SqlType(SqlTypeKind.BigInt),
            "binary" => new SqlType(SqlTypeKind.Binary, col.Length),
            "char" => new SqlType(SqlTypeKind.Char, col.Length),
            "nchar" => new SqlType(SqlTypeKind.NChar, col.Length),
            "varbinary" => new SqlType(SqlTypeKind.VarBinary, col.Length),
            "varchar" => new SqlType(SqlTypeKind.VarChar, col.Length),
            "bit" => new SqlType(SqlTypeKind.Bit),
            "nvarchar" => new SqlType(SqlTypeKind.NVarChar),
            "sysname" => new SqlType(SqlTypeKind.SysName),
            "uniqueidentifier" => new SqlType(SqlTypeKind.UniqueIdentifier),
            "datetime" => new SqlType(SqlTypeKind.DateTime),
            "smalldatetime" => new SqlType(SqlTypeKind.SmallDateTime),
            "sql_variant" => new SqlType(SqlTypeKind.SqlVariant),
            "image" => new SqlType(SqlTypeKind.Image),
            "ntext" => new SqlType(SqlTypeKind.NText),
            "float" => new SqlType(SqlTypeKind.Float),
            "real" => new SqlType(SqlTypeKind.Float),
            "money" => new SqlType(SqlTypeKind.BigInt), // Store as bigint (cents)
            "smallmoney" => new SqlType(SqlTypeKind.Int),
            "numeric" or "decimal" => new SqlType(SqlTypeKind.VarBinary, col.Length), // Fallback
            "date" => new SqlType(SqlTypeKind.DateTime),
            "time" => new SqlType(SqlTypeKind.DateTime),
            "datetime2" => new SqlType(SqlTypeKind.DateTime),
            "datetimeoffset" => new SqlType(SqlTypeKind.DateTime),
            "timestamp" or "rowversion" => new SqlType(SqlTypeKind.Binary, 8),
            "xml" => new SqlType(SqlTypeKind.NVarChar),
            "text" => new SqlType(SqlTypeKind.VarChar, col.Length),
            _ => new SqlType(SqlTypeKind.VarBinary, col.Length), // Unknown → BLOB
        };
    }

    public bool IsVarLength => Kind switch
    {
        SqlTypeKind.VarBinary or SqlTypeKind.VarChar or SqlTypeKind.SysName
            or SqlTypeKind.NVarChar or SqlTypeKind.SqlVariant
            or SqlTypeKind.Image or SqlTypeKind.NText => true,
        _ => false,
    };
}

// Tagged union for SQL values
public class SqlValue
{
    public SqlTypeKind Kind { get; }
    public long IntVal { get; init; }
    public double FloatVal { get; init; }
    public bool BoolVal { get; init; }
    public DateTime DateTimeVal { get; init; }
    public string? StringVal { get; init; }
    public byte[]? BlobVal { get; init; }
    public LobPointer? LobPtr { get; init; }
    public bool IsLob { get; init; }

    private SqlValue(SqlTypeKind kind) { Kind = kind; }

    public static SqlValue Null => new(SqlTypeKind.TinyInt) { IntVal = 0 };
    public static SqlValue FromTinyInt(sbyte v) => new(SqlTypeKind.TinyInt) { IntVal = v };
    public static SqlValue FromSmallInt(short v) => new(SqlTypeKind.SmallInt) { IntVal = v };
    public static SqlValue FromInt(int v) => new(SqlTypeKind.Int) { IntVal = v };
    public static SqlValue FromBigInt(long v) => new(SqlTypeKind.BigInt) { IntVal = v };
    public static SqlValue FromBit(bool v) => new(SqlTypeKind.Bit) { BoolVal = v };
    public static SqlValue FromFloat(double v) => new(SqlTypeKind.Float) { FloatVal = v };
    public static SqlValue FromDateTime(DateTime v) => new(SqlTypeKind.DateTime) { DateTimeVal = v };
    public static SqlValue FromGuid(byte[] v) => new(SqlTypeKind.UniqueIdentifier) { BlobVal = v };
    public static SqlValue FromString(SqlTypeKind kind, string v) => new(kind) { StringVal = v };
    public static SqlValue FromBlob(SqlTypeKind kind, byte[] v) => new(kind) { BlobVal = v };
    public static SqlValue FromLob(SqlTypeKind kind, LobPointer ptr) => new(kind) { LobPtr = ptr, IsLob = true };
}

public class BitParser
{
    private byte _currentByte;
    private int _readBits = 8;

    public bool ReadBit(ReadOnlySpan<byte> data, ref int cursor)
    {
        if (_readBits == 8)
        {
            _currentByte = data[cursor];
            cursor++;
            _readBits = 0;
        }

        bool ret = (_currentByte & 1) == 1;
        _currentByte >>= 1;
        _readBits++;
        return ret;
    }
}

public static class SqlTypeParser
{
    public static SqlValue ParseFixed(SqlType type, BitParser bitParser, ReadOnlySpan<byte> fixedData, ref int cursor)
    {
        switch (type.Kind)
        {
            case SqlTypeKind.TinyInt:
                return SqlValue.FromTinyInt((sbyte)fixedData[cursor++]);
            case SqlTypeKind.SmallInt:
            {
                var val = BinaryPrimitives.ReadInt16LittleEndian(fixedData[cursor..]);
                cursor += 2;
                return SqlValue.FromSmallInt(val);
            }
            case SqlTypeKind.Int:
            {
                var val = BinaryPrimitives.ReadInt32LittleEndian(fixedData[cursor..]);
                cursor += 4;
                return SqlValue.FromInt(val);
            }
            case SqlTypeKind.BigInt:
            {
                var val = BinaryPrimitives.ReadInt64LittleEndian(fixedData[cursor..]);
                cursor += 8;
                return SqlValue.FromBigInt(val);
            }
            case SqlTypeKind.Bit:
                return SqlValue.FromBit(bitParser.ReadBit(fixedData, ref cursor));
            case SqlTypeKind.Float:
            {
                var val = BinaryPrimitives.ReadDoubleLittleEndian(fixedData[cursor..]);
                cursor += 8;
                return SqlValue.FromFloat(val);
            }
            case SqlTypeKind.UniqueIdentifier:
            {
                var bytes = fixedData[cursor..(cursor + 16)].ToArray();
                cursor += 16;
                return SqlValue.FromGuid(bytes);
            }
            case SqlTypeKind.DateTime:
            {
                int time = BinaryPrimitives.ReadInt32LittleEndian(fixedData[cursor..]);
                int date = BinaryPrimitives.ReadInt32LittleEndian(fixedData[(cursor + 4)..]);
                cursor += 8;
                var dt = new DateTime(1900, 1, 1);
                if (date is > 0 and < 1_000_000)
                    dt = dt.AddDays(date);
                dt = dt.AddMilliseconds((long)time * 1000 / 300);
                return SqlValue.FromDateTime(dt);
            }
            case SqlTypeKind.SmallDateTime:
            {
                ushort time = BinaryPrimitives.ReadUInt16LittleEndian(fixedData[cursor..]);
                ushort date = BinaryPrimitives.ReadUInt16LittleEndian(fixedData[(cursor + 2)..]);
                cursor += 4;
                var dt = new DateTime(1900, 1, 1).AddDays(date).AddMinutes(time);
                return SqlValue.FromDateTime(dt);
            }
            case SqlTypeKind.Binary:
            {
                var bytes = fixedData[cursor..(cursor + type.Size)].ToArray();
                cursor += type.Size;
                return SqlValue.FromBlob(SqlTypeKind.Binary, bytes);
            }
            case SqlTypeKind.Char:
            {
                var str = Encoding.UTF8.GetString(fixedData[cursor..(cursor + type.Size)]);
                cursor += type.Size;
                return SqlValue.FromString(SqlTypeKind.Char, str);
            }
            case SqlTypeKind.NChar:
            {
                var str = Encoding.Unicode.GetString(fixedData[cursor..(cursor + type.Size)]);
                cursor += type.Size;
                return SqlValue.FromString(SqlTypeKind.NChar, str);
            }
            default:
                throw new InvalidDataException($"Cannot parse fixed-length type: {type.Kind}");
        }
    }

    public static SqlValue ParseVarLength(SqlType type, bool complex, ReadOnlyMemory<byte> data)
    {
        var span = data.Span;
        switch (type.Kind)
        {
            case SqlTypeKind.VarBinary:
                if (complex)
                    return SqlValue.FromLob(SqlTypeKind.VarBinary, LobPointer.Parse(span));
                return SqlValue.FromBlob(SqlTypeKind.VarBinary, span.ToArray());

            case SqlTypeKind.VarChar:
            {
                // Try UTF-8, fall back to Windows-1252
                try
                {
                    var str = Encoding.UTF8.GetString(span);
                    return SqlValue.FromString(SqlTypeKind.VarChar, str);
                }
                catch
                {
                    var str = Encoding.GetEncoding(1252).GetString(span);
                    return SqlValue.FromString(SqlTypeKind.VarChar, str);
                }
            }

            case SqlTypeKind.SysName:
                return SqlValue.FromString(SqlTypeKind.SysName, Encoding.Unicode.GetString(span));

            case SqlTypeKind.NVarChar:
                if (complex)
                    return SqlValue.FromLob(SqlTypeKind.NVarChar, LobPointer.Parse(span));
                return SqlValue.FromString(SqlTypeKind.NVarChar, Encoding.Unicode.GetString(span));

            case SqlTypeKind.SqlVariant:
                return SqlValue.FromBlob(SqlTypeKind.SqlVariant, span.ToArray());

            case SqlTypeKind.Image:
            case SqlTypeKind.NText:
                if (span.Length >= 16 && complex)
                    return SqlValue.FromLob(type.Kind, LobPointer.Parse(span));
                if (span.IsEmpty)
                    return SqlValue.FromBlob(type.Kind, []);
                return SqlValue.FromBlob(type.Kind, span.ToArray());

            default:
                throw new InvalidDataException($"Cannot parse var-length type: {type.Kind}");
        }
    }
}
