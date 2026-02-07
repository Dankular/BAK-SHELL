using System.Buffers.Binary;
using System.Text;

namespace BakShell.Mdf;

// Well-known allocation unit and row set IDs
public static class WellKnownIds
{
    public const long SysRowSetAuId = 327680;
    public const int SysSchObjsIdMajor = 34;
    public const int SysColParsIdMajor = 41;
    public const int SysScalarTypesIdMajor = 50;
    public const int SysSingleObjectRefsIdMajor = 74;
}

public enum AllocUnitType : byte { Dropped = 0, InRowData = 1, LobData = 2, RowOverflowData = 3 }

public class SysAllocUnit
{
    public long AuId { get; init; }
    public AllocUnitType Type { get; init; }
    public long OwnerId { get; init; }
    public PagePointer? PgFirst { get; init; }
    public PagePointer? PgRoot { get; init; }
    public PagePointer? PgFirstIam { get; init; }

    private static readonly Schema _schema = new(new List<ColumnType>
    {
        new() { Idx = 0, DataType = new SqlType(SqlTypeKind.BigInt), Name = "au_id" },
        new() { Idx = 1, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "ty" },
        new() { Idx = 2, DataType = new SqlType(SqlTypeKind.BigInt), Name = "owner_id" },
        new() { Idx = 3, DataType = new SqlType(SqlTypeKind.Int), Name = "status" },
        new() { Idx = 4, DataType = new SqlType(SqlTypeKind.SmallInt), Name = "fgid" },
        new() { Idx = 5, DataType = new SqlType(SqlTypeKind.Binary, 6), Name = "pg_first" },
        new() { Idx = 6, DataType = new SqlType(SqlTypeKind.Binary, 6), Name = "pg_root" },
        new() { Idx = 7, DataType = new SqlType(SqlTypeKind.Binary, 6), Name = "pg_firstiam" },
        new() { Idx = 8, DataType = new SqlType(SqlTypeKind.BigInt), Name = "pc_used" },
        new() { Idx = 9, DataType = new SqlType(SqlTypeKind.BigInt), Name = "pc_data" },
        new() { Idx = 10, DataType = new SqlType(SqlTypeKind.BigInt), Name = "pc_reserved" },
        new() { Idx = 11, DataType = new SqlType(SqlTypeKind.Int), Name = "db_frag_id", Nullable = true },
    });

    public static SysAllocUnit Parse(Record record)
    {
        var row = _schema.ParseRow(record);
        return new SysAllocUnit
        {
            AuId = row[0]?.IntVal ?? 0,
            Type = (AllocUnitType)(byte)(row[1]?.IntVal ?? 0),
            OwnerId = row[2]?.IntVal ?? 0,
            PgFirst = row[5]?.BlobVal != null ? PagePointer.Parse(row[5]!.BlobVal) : null,
            PgRoot = row[6]?.BlobVal != null ? PagePointer.Parse(row[6]!.BlobVal) : null,
            PgFirstIam = row[7]?.BlobVal != null ? PagePointer.Parse(row[7]!.BlobVal) : null,
        };
    }
}

public class SysRowSet
{
    public long RowSetId { get; init; }
    public int IdMajor { get; init; }
    public int IdMinor { get; init; }

    private static readonly Schema _schema = new(new List<ColumnType>
    {
        new() { Idx = 0, DataType = new SqlType(SqlTypeKind.BigInt), Name = "row_set_id" },
        new() { Idx = 1, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "owner_type" },
        new() { Idx = 2, DataType = new SqlType(SqlTypeKind.Int), Name = "id_major" },
        new() { Idx = 3, DataType = new SqlType(SqlTypeKind.Int), Name = "id_minor" },
        new() { Idx = 4, DataType = new SqlType(SqlTypeKind.Int), Name = "num_part" },
        new() { Idx = 5, DataType = new SqlType(SqlTypeKind.Int), Name = "status" },
        new() { Idx = 6, DataType = new SqlType(SqlTypeKind.SmallInt), Name = "fgidfs" },
        new() { Idx = 7, DataType = new SqlType(SqlTypeKind.BigInt), Name = "rcrows" },
        new() { Idx = 8, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "cmpr_level", Nullable = true },
        new() { Idx = 9, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "fill_fact", Nullable = true },
        new() { Idx = 10, DataType = new SqlType(SqlTypeKind.Int), Name = "max_leaf", Nullable = true },
        new() { Idx = 11, DataType = new SqlType(SqlTypeKind.SmallInt), Name = "max_int", Nullable = true },
        new() { Idx = 12, DataType = new SqlType(SqlTypeKind.SmallInt), Name = "min_leaf", Nullable = true },
        new() { Idx = 13, DataType = new SqlType(SqlTypeKind.SmallInt), Name = "min_int", Nullable = true },
        new() { Idx = 14, DataType = new SqlType(SqlTypeKind.VarBinary), Name = "rs_guid", Nullable = true },
        new() { Idx = 15, DataType = new SqlType(SqlTypeKind.VarBinary), Name = "lock_res", Nullable = true },
        new() { Idx = 16, DataType = new SqlType(SqlTypeKind.Int), Name = "db_frag_id", Nullable = true },
    });

    public static SysRowSet Parse(Record record)
    {
        var row = _schema.ParseRow(record);
        return new SysRowSet
        {
            RowSetId = row[0]?.IntVal ?? 0,
            IdMajor = (int)(row[2]?.IntVal ?? 0),
            IdMinor = (int)(row[3]?.IntVal ?? 0),
        };
    }
}

public enum SchType { SystemTable, UserTable, Other }

public class SysSchObj
{
    public int Id { get; init; }
    public string Name { get; init; } = "";
    public SchType Type { get; init; }

    private static readonly Schema _schema = new(new List<ColumnType>
    {
        new() { Idx = 0, DataType = new SqlType(SqlTypeKind.Int), Name = "id" },
        new() { Idx = 1, DataType = new SqlType(SqlTypeKind.SysName), Name = "name" },
        new() { Idx = 2, DataType = new SqlType(SqlTypeKind.Int), Name = "ns_id" },
        new() { Idx = 3, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "ns_class" },
        new() { Idx = 4, DataType = new SqlType(SqlTypeKind.Int), Name = "status" },
        new() { Idx = 5, DataType = new SqlType(SqlTypeKind.Char, 2), Name = "ty" },
        new() { Idx = 6, DataType = new SqlType(SqlTypeKind.Int), Name = "pid" },
        new() { Idx = 7, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "pcall" },
        new() { Idx = 8, DataType = new SqlType(SqlTypeKind.Int), Name = "int_prop" },
        new() { Idx = 9, DataType = new SqlType(SqlTypeKind.DateTime), Name = "created" },
        new() { Idx = 10, DataType = new SqlType(SqlTypeKind.DateTime), Name = "modified" },
    });

    public static SysSchObj Parse(Record record)
    {
        var row = _schema.ParseRow(record);
        string typeCode = row[5]?.StringVal?.TrimEnd() ?? "";
        var schType = typeCode switch
        {
            "S" => SchType.SystemTable,
            "U" => SchType.UserTable,
            _ => SchType.Other,
        };
        return new SysSchObj
        {
            Id = (int)(row[0]?.IntVal ?? 0),
            Name = row[1]?.StringVal?.TrimEnd('\0') ?? "",
            Type = schType,
        };
    }
}

[Flags]
public enum ColParStatus
{
    Nullable = 1 << 0,
    Computed = 1 << 4,
    Sparse = 1 << 24,
}

public class SysColPar
{
    public int Id { get; init; }
    public int ColId { get; init; }
    public string? Name { get; init; }
    public sbyte XType { get; init; }
    public short Length { get; init; }
    public ColParStatus Status { get; init; }

    public bool IsComputed => Status.HasFlag(ColParStatus.Computed);

    private static readonly Schema _schema = new(new List<ColumnType>
    {
        new() { Idx = 0, DataType = new SqlType(SqlTypeKind.Int), Name = "id" },
        new() { Idx = 1, DataType = new SqlType(SqlTypeKind.SmallInt), Name = "number" },
        new() { Idx = 2, DataType = new SqlType(SqlTypeKind.Int), Name = "col_id" },
        new() { Idx = 3, DataType = new SqlType(SqlTypeKind.SysName), Name = "name", Nullable = true },
        new() { Idx = 4, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "xtype" },
        new() { Idx = 5, DataType = new SqlType(SqlTypeKind.Int), Name = "utype" },
        new() { Idx = 6, DataType = new SqlType(SqlTypeKind.SmallInt), Name = "length" },
        new() { Idx = 7, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "prec" },
        new() { Idx = 8, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "scale" },
        new() { Idx = 9, DataType = new SqlType(SqlTypeKind.Int), Name = "collation_id" },
        new() { Idx = 10, DataType = new SqlType(SqlTypeKind.Int), Name = "status" },
        new() { Idx = 11, DataType = new SqlType(SqlTypeKind.SmallInt), Name = "max_in_row" },
        new() { Idx = 12, DataType = new SqlType(SqlTypeKind.Int), Name = "xml_ns" },
        new() { Idx = 13, DataType = new SqlType(SqlTypeKind.Int), Name = "dflt" },
        new() { Idx = 14, DataType = new SqlType(SqlTypeKind.Int), Name = "chk" },
        new() { Idx = 15, DataType = new SqlType(SqlTypeKind.VarBinary), Name = "idt_val", Nullable = true },
    });

    public static SysColPar Parse(Record record)
    {
        var row = _schema.ParseRow(record);
        return new SysColPar
        {
            Id = (int)(row[0]?.IntVal ?? 0),
            ColId = (int)(row[2]?.IntVal ?? 0),
            Name = row[3]?.StringVal?.TrimEnd('\0'),
            XType = (sbyte)(row[4]?.IntVal ?? 0),
            Length = (short)(row[6]?.IntVal ?? 0),
            Status = (ColParStatus)(int)(row[10]?.IntVal ?? 0),
        };
    }
}

public class SysScalarType
{
    public int Id { get; init; }
    public string Name { get; init; } = "";
    public sbyte XType { get; init; }

    private static readonly Schema _schema = new(new List<ColumnType>
    {
        new() { Idx = 0, DataType = new SqlType(SqlTypeKind.Int), Name = "id" },
        new() { Idx = 1, DataType = new SqlType(SqlTypeKind.Int), Name = "sch_id" },
        new() { Idx = 2, DataType = new SqlType(SqlTypeKind.SysName), Name = "name" },
        new() { Idx = 3, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "xtype" },
        new() { Idx = 4, DataType = new SqlType(SqlTypeKind.SmallInt), Name = "length" },
        new() { Idx = 5, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "prec" },
        new() { Idx = 6, DataType = new SqlType(SqlTypeKind.TinyInt), Name = "scale" },
        new() { Idx = 7, DataType = new SqlType(SqlTypeKind.Int), Name = "collation_id" },
        new() { Idx = 8, DataType = new SqlType(SqlTypeKind.Int), Name = "status" },
        new() { Idx = 9, DataType = new SqlType(SqlTypeKind.DateTime), Name = "created" },
        new() { Idx = 10, DataType = new SqlType(SqlTypeKind.DateTime), Name = "modified" },
        new() { Idx = 11, DataType = new SqlType(SqlTypeKind.Int), Name = "dflt" },
        new() { Idx = 12, DataType = new SqlType(SqlTypeKind.Int), Name = "chk" },
    });

    public static SysScalarType Parse(Record record)
    {
        var row = _schema.ParseRow(record);
        return new SysScalarType
        {
            Id = (int)(row[0]?.IntVal ?? 0),
            Name = row[2]?.StringVal?.TrimEnd('\0') ?? "",
            XType = (sbyte)(row[3]?.IntVal ?? 0),
        };
    }
}
