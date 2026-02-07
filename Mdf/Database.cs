using System.Buffers.Binary;
using System.Text;

namespace BakShell.Mdf;

public class BootPage
{
    public PagePointer FirstSysIndices { get; init; }
    public string DatabaseName { get; init; } = "";

    public static BootPage Parse(RawPage page)
    {
        var record = page.GetRecord(0);
        if (record == null)
            throw new InvalidDataException("Boot page has no records");

        var fixedData = record.FixedData.Span;

        // Database name is at offset 48-304, UTF-16LE
        string dbName = "";
        if (fixedData.Length >= 304)
            dbName = Encoding.Unicode.GetString(fixedData[48..304]).TrimEnd('\0');

        // First sys indices pointer at offset 512-518
        PagePointer? firstSysIndices = null;
        if (fixedData.Length >= 518)
            firstSysIndices = PagePointer.Parse(fixedData[512..518]);

        return new BootPage
        {
            FirstSysIndices = firstSysIndices ?? throw new InvalidDataException("Boot page missing first_sys_indices"),
            DatabaseName = dbName,
        };
    }
}

public class MdfDatabase
{
    public IPageProvider PageProvider { get; }
    public BootPage Boot { get; }
    public List<SysAllocUnit> AllocUnits { get; }
    public List<SysRowSet> RowSets { get; }
    public List<SysSchObj> SchObjs { get; }
    public List<SysColPar> ColPars { get; }
    public List<SysScalarType> ScalarTypes { get; }

    public MdfDatabase(IPageProvider pageProvider)
    {
        PageProvider = pageProvider;

        // Read boot page at (file_id=1, page_id=9)
        var bootPage = pageProvider.GetPage(new PagePointer(9, 1))
            ?? throw new InvalidDataException("Cannot read boot page (file 1, page 9)");
        Boot = BootPage.Parse(bootPage);

        Console.WriteLine($"  Database: {Boot.DatabaseName}");
        Console.WriteLine("  Reading system tables...");

        // Read allocation units from the first sys indices page
        var allocPage = pageProvider.GetPage(Boot.FirstSysIndices)
            ?? throw new InvalidDataException($"Cannot read alloc units page {Boot.FirstSysIndices}");
        AllocUnits = allocPage.Records().Select(SysAllocUnit.Parse).ToList();
        Console.WriteLine($"    {AllocUnits.Count} allocation units");

        // Read row sets
        var rowSetAu = FindAllocUnitById(WellKnownIds.SysRowSetAuId, AllocUnitType.InRowData)
            ?? throw new InvalidDataException("Cannot find SysRowSet allocation unit");
        var rowSetPage = pageProvider.GetPage(rowSetAu.PgFirst!.Value)
            ?? throw new InvalidDataException("Cannot read SysRowSet page");
        RowSets = rowSetPage.Records().Select(SysRowSet.Parse).ToList();
        Console.WriteLine($"    {RowSets.Count} row sets");

        // Read schema objects (tables, views, etc.)
        SchObjs = ReadSystemTable(WellKnownIds.SysSchObjsIdMajor, SysSchObj.Parse);
        Console.WriteLine($"    {SchObjs.Count} schema objects");

        // Read column parameters
        ColPars = ReadSystemTable(WellKnownIds.SysColParsIdMajor, SysColPar.Parse);
        Console.WriteLine($"    {ColPars.Count} column parameters");

        // Read scalar types
        ScalarTypes = ReadSystemTable(WellKnownIds.SysScalarTypesIdMajor, SysScalarType.Parse);
        Console.WriteLine($"    {ScalarTypes.Count} scalar types");
    }

    private List<T> ReadSystemTable<T>(int idMajor, Func<Record, T> parser)
    {
        var au = FindAllocUnitByRowSet(idMajor, 1);
        if (au?.PgFirst == null) return new List<T>();
        var page = PageProvider.GetPage(au.PgFirst.Value);
        if (page == null) return new List<T>();
        return page.Records().Select(parser).ToList();
    }

    private SysAllocUnit? FindAllocUnitById(long auId, AllocUnitType type)
    {
        return AllocUnits.FirstOrDefault(au => au.AuId == auId && au.Type == type);
    }

    private SysAllocUnit? FindAllocUnitByRowSet(int idMajor, int idMinor)
    {
        var rowSet = RowSets.FirstOrDefault(rs => rs.IdMajor == idMajor && rs.IdMinor == idMinor);
        if (rowSet == null) return null;
        return AllocUnits.FirstOrDefault(au => au.OwnerId == rowSet.RowSetId && au.Type == AllocUnitType.InRowData);
    }

    public IEnumerable<SysSchObj> UserTables()
    {
        return SchObjs.Where(obj => obj.Type == SchType.UserTable);
    }

    public IEnumerable<SysSchObj> AllTables()
    {
        return SchObjs.Where(obj => obj.Type is SchType.UserTable or SchType.SystemTable);
    }

    public IEnumerable<SysColPar> ColumnsForTable(SysSchObj table)
    {
        return ColPars.Where(col => col.Id == table.Id);
    }

    public SysScalarType? TypeForColumn(SysColPar col)
    {
        return ScalarTypes.FirstOrDefault(ty => ty.XType == col.XType && ty.Id <= 255);
    }

    public MdfTable? GetTable(string name)
    {
        var obj = SchObjs.FirstOrDefault(o => o.Name == name && o.Type == SchType.UserTable);
        if (obj == null) return null;
        return BuildTable(obj);
    }

    public MdfTable BuildTable(SysSchObj obj)
    {
        var columns = ColumnsForTable(obj)
            .Select(col =>
            {
                var scalarType = TypeForColumn(col);
                if (scalarType == null) return null;
                return new ColumnType
                {
                    Idx = col.ColId,
                    DataType = SqlType.FromColumn(col, scalarType),
                    Name = col.Name ?? "",
                    Nullable = !col.Status.HasFlag(ColParStatus.Nullable),
                    Computed = col.IsComputed,
                };
            })
            .Where(c => c != null)
            .Select(c => c!)
            .ToList();

        var schema = new Schema(columns);

        // Get partition page pointers
        var partitions = RowSets
            .Where(rs => rs.IdMajor == obj.Id && rs.IdMinor <= 1)
            .Select(rs => AllocUnits.FirstOrDefault(au => au.OwnerId == rs.RowSetId && au.Type == AllocUnitType.InRowData))
            .Where(au => au?.PgFirst != null)
            .Select(au => au!.PgFirst!.Value)
            .ToList();

        return new MdfTable(obj.Name, PageProvider, schema, partitions);
    }
}

public class MdfTable
{
    public string Name { get; }
    public IPageProvider PageProvider { get; }
    public Schema Schema { get; }
    public List<PagePointer> PartitionPointers { get; }

    public MdfTable(string name, IPageProvider pageProvider, Schema schema, List<PagePointer> partitionPointers)
    {
        Name = name;
        PageProvider = pageProvider;
        Schema = schema;
        PartitionPointers = partitionPointers;
    }

    public IEnumerable<SqlValue?[]> Rows()
    {
        foreach (var partPtr in PartitionPointers)
        {
            var page = PageProvider.GetPage(partPtr);
            if (page == null) continue;

            foreach (var record in page.Records())
            {
                SqlValue?[] row;
                try
                {
                    row = Schema.ParseRow(record);
                }
                catch
                {
                    continue; // Skip broken records
                }
                yield return row;
            }
        }
    }
}
