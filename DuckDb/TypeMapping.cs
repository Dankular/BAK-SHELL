using System.Text;
using BakShell.Mdf;

namespace BakShell.DuckDb;

public static class TypeMapping
{
    public static string SqlTypeToDuckDb(SqlType type)
    {
        return type.Kind switch
        {
            SqlTypeKind.TinyInt => "TINYINT",
            SqlTypeKind.SmallInt => "SMALLINT",
            SqlTypeKind.Int => "INTEGER",
            SqlTypeKind.BigInt => "BIGINT",
            SqlTypeKind.Bit => "BOOLEAN",
            SqlTypeKind.Float => "DOUBLE",
            SqlTypeKind.DateTime or SqlTypeKind.SmallDateTime => "TIMESTAMP",
            SqlTypeKind.UniqueIdentifier => "VARCHAR",
            SqlTypeKind.Char or SqlTypeKind.NChar or SqlTypeKind.VarChar
                or SqlTypeKind.NVarChar or SqlTypeKind.SysName or SqlTypeKind.NText => "VARCHAR",
            SqlTypeKind.Binary or SqlTypeKind.VarBinary
                or SqlTypeKind.Image or SqlTypeKind.SqlVariant => "BLOB",
            _ => "VARCHAR",
        };
    }

    public static string FormatUuid(byte[] bytes)
    {
        // SQL Server mixed-endian UUID format
        return $"{bytes[3]:x2}{bytes[2]:x2}{bytes[1]:x2}{bytes[0]:x2}" +
               $"-{bytes[5]:x2}{bytes[4]:x2}" +
               $"-{bytes[7]:x2}{bytes[6]:x2}" +
               $"-{bytes[8]:x2}{bytes[9]:x2}" +
               $"-{bytes[10]:x2}{bytes[11]:x2}{bytes[12]:x2}{bytes[13]:x2}{bytes[14]:x2}{bytes[15]:x2}";
    }

    public static object? ConvertToDuckDbValue(SqlValue? value, IPageProvider pageProvider)
    {
        if (value == null) return null;

        // Handle LOB values
        if (value.IsLob && value.LobPtr != null)
        {
            var lobData = value.LobPtr.Read(pageProvider);
            if (lobData == null) return null;

            if (value.Kind is SqlTypeKind.NVarChar or SqlTypeKind.NText)
            {
                // NVarChar/NText LOBs are UTF-16LE
                return Encoding.Unicode.GetString(lobData);
            }
            return lobData; // Binary LOB
        }

        return value.Kind switch
        {
            SqlTypeKind.TinyInt => (sbyte)value.IntVal,
            SqlTypeKind.SmallInt => (short)value.IntVal,
            SqlTypeKind.Int => (int)value.IntVal,
            SqlTypeKind.BigInt => value.IntVal,
            SqlTypeKind.Bit => value.BoolVal,
            SqlTypeKind.Float => value.FloatVal,
            SqlTypeKind.DateTime or SqlTypeKind.SmallDateTime =>
                value.DateTimeVal,
            SqlTypeKind.UniqueIdentifier =>
                value.BlobVal != null && value.BlobVal.Length == 16 ? FormatUuid(value.BlobVal) : null,
            SqlTypeKind.Char or SqlTypeKind.NChar =>
                value.StringVal?.TrimEnd(),
            SqlTypeKind.VarChar or SqlTypeKind.NVarChar or SqlTypeKind.SysName =>
                value.StringVal,
            SqlTypeKind.Binary or SqlTypeKind.VarBinary or SqlTypeKind.Image or SqlTypeKind.SqlVariant =>
                value.BlobVal,
            SqlTypeKind.NText =>
                value.BlobVal != null ? Encoding.Unicode.GetString(value.BlobVal) : null,
            _ => value.StringVal ?? (object?)value.BlobVal,
        };
    }
}
