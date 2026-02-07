# BAK-SHELL - Claude Development Guide

This file provides context for Claude Code when working on the BAK-SHELL project.

## Project Overview

**BAK-SHELL** is a C# .NET 8.0 command-line tool that extracts data from SQL Server `.BAK` backup files and loads it into DuckDB databases. It parses the MTF (Microsoft Tape Format) container and MDF (SQL Server database page format) without requiring a SQL Server instance.

**Modes**:
- **Interactive REPL** (recommended): Parse backup once, then run unlimited queries/commands
- **Batch extraction**: One-shot extraction of tables to DuckDB file

**Location**: `\\efret-hpv-02\Development Share\SQLRestore\BAK-SHELL\`

**Test file**: `Z:\SQLRestore\axs_db_202602040915.BAK` (1.3GB, 71 user tables, 2.9M rows)

## Architecture

```
.BAK File (MTF Container)
    ├─ MtfParser.FindMqdaStream() → finds MQDA stream offset/length
    └─ MQDA Stream (MDF Pages)
        ├─ MtfPageProvider.BuildPageIndex() → indexes all pages by (fileId, pageId)
        └─ MdfDatabase
            ├─ Boot page (1:9) → database name + first_sys_indices pointer
            ├─ System tables (SysSchObj, SysColPar, SysAllocUnit, etc.)
            └─ User tables
                ├─ MdfTable.Rows() → iterates data pages
                └─ DuckDbLoader.LoadTable() → bulk loads via Appender API
```

## Key Implementation Details

### MTF Format Parsing (`Mtf/MtfParser.cs`)

**Critical details**:
1. **Block structure**: 52-byte DBLK header + variable-length body + streams
2. **Checksum**: XOR-only (not XOR+SUM) - all 16-bit words must XOR to 0
3. **Unknown block types**: SQL Server uses Microsoft extensions (MSCI, MQDA) not in standard MTF
   - **DO NOT** reject blocks with unknown type IDs
   - Validate using checksum only
4. **MQDA stream**: Contains all MDF pages
   - Has a 2-byte header at the very start (skip it: `return (dataStart + 2, dataLength - 2)`)
   - Then sequential 8KB pages with no separators
5. **Stream iteration**: Check if a stream header is actually a DBLK using `IsKnownDblkType()`
   - Known DBLK types: TAPE, SSET, VOLB, DIRB, FILE, CFIL, ESPB, ESET, EOTM, SFMB
   - Stream IDs are also 4 uppercase ASCII letters (MQDA, SPAD, RAID) - can't use "all uppercase" heuristic

**Reference**: `deps/mtf-rs/src/lib.rs` in original Rust implementation

### Page Provider (`Mtf/MtfPageProvider.cs`)

**Purpose**: Provides efficient random access to MDF pages within the MQDA stream.

**Implementation**:
- Uses `MemoryMappedFile` for efficient large file access (DO NOT load entire file into memory)
- `BuildPageIndex()`: Scans MQDA stream sequentially, reads page headers (96 bytes)
  - pageId at offset 32 (4 bytes, little-endian)
  - fileId at offset 36 (2 bytes, little-endian)
  - Stores `Dictionary<(ushort fileId, uint pageId), long physicalOffset>`
- `GetPage(PagePointer)`: Looks up offset, creates 8KB `ViewAccessor`, returns `RawPage`

**Reference**: `deps/mtf-rs/src/mdf.rs:39-203`

### MDF Page Format (`Mdf/RawPage.cs`)

**Page structure** (8192 bytes):
- Bytes 0-95: Page header (contains page type, slot count)
- Bytes 96+: Data region
- Last N*2 bytes: Slot array (offset pointers to records)

**Record access**: `GetRecord(slotId)` reads slot offset, then `Record.Parse()` from that position

**Reference**: `deps/mtf-rs/mdf/src/raw_page.rs:96-261`

### Record Format (`Mdf/Record.cs`)

**Record layout**:
```
[StatusBitsA: 1 byte]
[FixedData: variable length]
[ColumnCount: 2 bytes]
[NullBitmap: ceil(ColumnCount/8) bytes]
[VarColumnCount: 2 bytes]
[OffsetArray: VarColumnCount * 2 bytes]
[VarData: variable length]
```

**Key details**:
- Fixed-length columns are always present (use null bitmap for NULL values)
- Variable-length columns have offsets relative to start of VarData region
- Bit columns share bytes (8 bits per byte) - need bit-level cursor

**Reference**: `deps/mtf-rs/mdf/src/record.rs:1-180`

### Type System (`Mdf/SqlType.cs`)

**SqlValue union** represents parsed values:
```csharp
public class SqlValue
{
    public SqlTypeKind Kind;
    public bool IsNull;
    public bool IsLob;              // Out-of-row data
    public LobPointer? LobPtr;      // If IsLob=true
    public long IntVal;             // Int types
    public double FloatVal;         // Float types
    public bool BoolVal;            // Bit
    public DateTime DateTimeVal;    // DateTime/SmallDateTime
    public string? StringVal;       // String types
    public byte[]? BlobVal;         // Binary types, UUID
}
```

**DateTime parsing** (4 bytes date + 4 bytes time):
- Date: days since 1900-01-01
- Time: 1/300th second ticks since midnight
- SmallDateTime: minutes since 1900-01-01 (2 bytes date + 2 bytes time)

**UUID/UNIQUEIDENTIFIER**: 16 bytes, mixed-endian format - use `new Guid(ReadOnlySpan<byte>)`

**String encoding**:
- VarChar/Char: Try UTF-8, fallback to Windows-1252 (`Encoding.GetEncoding(1252)`)
- NVarChar/NChar: UTF-16LE
- SysName: UTF-16LE (system object names)

**Reference**: `deps/mtf-rs/mdf/src/types.rs:1-471`

### System Tables (`Mdf/Database.cs`)

**Bootstrap sequence**:
1. Read boot page at `(fileId=1, pageId=9)`
   - Database name at bytes 48-304 (UTF-16LE, truncate at first null)
   - FirstSysIndices pointer at bytes 512-518
2. Read `SysAllocUnits` from FirstSysIndices page
3. Find allocation units for each system table by rowset ID
4. Parse system tables:
   - `SysSchObj` - schema objects (tables, views)
   - `SysColPar` - columns and parameters
   - `SysScalarType` - type definitions
   - `SysRowSet` - partition/rowset metadata

**Well-known IDs** (`WellKnownIds` class):
- SysRowSetAuId: 1 (allocation unit for SysRowSet itself)
- SysSchObjsIdMajor: 34 (SysSchObj table)
- SysColParsIdMajor: 41 (SysColPar table)
- SysScalarTypesIdMajor: 50 (SysScalarType table)

**Reference**: `deps/mtf-rs/mdf/src/db.rs:1-130`, `deps/mtf-rs/mdf/src/system_tables.rs:1-250`

### DuckDB Integration (`DuckDb/DuckDbLoader.cs`)

**Appender API usage** (CRITICAL for performance):
```csharp
using var appender = _conn.CreateAppender(tableName);
foreach (var row in table.Rows())
{
    var appRow = appender.CreateRow();  // Returns IDuckDBAppenderRow

    // Must use typed AppendValue - DateTime MUST be actual DateTime, not string
    appRow.AppendValue(intValue);
    appRow.AppendValue(stringValue);
    appRow.AppendValue(dateTimeValue);  // Pass DateTime object
    appRow.AppendNullValue();

    appRow.EndRow();
}
```

**Type conversion** (`TypeMapping.cs`):
- `SqlTypeToDuckDb(SqlType)`: Returns DuckDB type string for DDL
- `ConvertToDuckDbValue(SqlValue)`: Converts SqlValue to DuckDB-compatible object
  - **CRITICAL**: DateTime/SmallDateTime MUST return `DateTime` object (not formatted string)
  - UniqueIdentifier: Convert to UUID string format
  - LOB values: Resolve via `LobPointer.Read(pageProvider)`

**AppendValueToRow** must have explicit handler for DateTime:
```csharp
case DateTime dt:
    row.AppendValue(dt);
    break;
```

**Fallback**: If Appender fails, falls back to parameterized INSERT (3-5x slower)

**Reference**: `src/duckdb_loader.rs:1-142`, `src/type_mapping.rs:1-160`

## Common Issues and Solutions

### MTF Parsing Errors

**"No database stream (MQDA) found"**
- Cause: Parser stopped at unknown DBLK type
- Fix: Remove `IsKnownDblkType()` check in outer DBLK loop, use checksum validation only

**"Cannot read boot page"**
- Cause: Forgot to skip 2-byte MQDA header
- Fix: Ensure `FindMqdaStream()` returns `(dataStart + 2, dataLength - 2)`

### Type Conversion Errors

**"Cannot write String to Timestamp column"**
- Cause: `TypeMapping.ConvertToDuckDbValue()` returns formatted string for DateTime
- Fix: Return actual `DateTime` object, ensure `AppendValueToRow()` has `case DateTime` handler

**"Attempted to read or write protected memory" (segfault)**
- Cause: Passing wrong type to Appender API
- Fix: Ensure all type conversions are correct, particularly DateTime handling

### Performance Issues

**Slow on network shares**
- Network I/O is bottleneck
- Solution: Copy `.BAK` file to local drive first

## Development Guidelines

### When Modifying Code

1. **MTF parsing**: Always refer to `deps/mtf-rs/src/lib.rs` for format details
2. **MDF parsing**: Refer to `deps/mtf-rs/mdf/src/*.rs` for page/record structure
3. **Do NOT guess** - SQL Server's formats are complex and undocumented
4. **Test with real data**: Use `axs_db_202602040915.BAK` for verification

### Testing Workflow

```bash
# List tables
dotnet run -- "Z:\SQLRestore\axs_db_202602040915.BAK" --list

# Single table test
dotnet run -- "Z:\SQLRestore\axs_db_202602040915.BAK" -t Company -o test.duckdb

# Verify extraction
dotnet run -- "Z:\SQLRestore\axs_db_202602040915.BAK" -o test.duckdb -q "SELECT COUNT(*) FROM Company"

# Full extraction (should complete in ~60-70s, 2.9M rows)
dotnet run -- "Z:\SQLRestore\axs_db_202602040915.BAK" -o full.duckdb
```

### Adding New SQL Types

1. Add to `SqlTypeKind` enum in `SqlType.cs`
2. Add parsing logic in appropriate `Parse*` method
3. Add to type mapping in `TypeMapping.SqlTypeToDuckDb()`
4. Add conversion in `TypeMapping.ConvertToDuckDbValue()`
5. Test with table containing that type

### Adding Features

**DO**:
- Read existing code patterns before making changes
- Use memory-mapped files for large data access
- Follow existing error handling patterns (try/catch with error count)
- Add progress output for long operations (every 100K rows)

**DON'T**:
- Load entire files into memory
- Use subagents for code generation (see MEMORY.md)
- Guess at binary format details - refer to Rust implementation
- Skip proper type conversions (especially DateTime)

## Performance Expectations

- **Page indexing**: ~0.5-1 second for 162K pages
- **System table parsing**: <1 second
- **Data extraction**: ~40-50K rows/second (network share), faster on local disk
- **Full database** (71 tables, 2.9M rows): ~60 seconds

## Related Files

- `MEMORY.md` - Lessons learned, critical implementation notes
- Plan file: `C:\Users\sysadmin\.claude\plans\gleaming-noodling-wombat.md`
- Rust reference: `\\efret-hpv-02\Development Share\SQLRestore\bak2duckdb\` (original implementation)

## Dependencies

```xml
<PackageReference Include="DuckDB.NET.Data.Full" Version="1.4.3" />
<PackageReference Include="System.Text.Encoding.CodePages" Version="8.0.0" />
```

Keep dependencies minimal - all binary parsing uses BCL types (`BinaryPrimitives`, `MemoryMappedFile`, `Span<byte>`).

## Interactive REPL Architecture

### Overview

The REPL (Read-Eval-Print Loop) mode allows users to explore backups interactively without re-parsing the backup file for each operation. All components are initialized once and kept alive throughout the session.

### Components

**Repl/ReplSession.cs** - Session manager
- Owns: MtfParser, MtfPageProvider, MdfDatabase, DuckDbLoader
- Provides: `ListTables()`, `GetTableSchema()`, `ExecuteQuery()`, `EnsureTableLoaded()`, `LoadAllTables()`
- Lifecycle: Created once at interactive mode start, disposed on exit
- Memory: ~30-50MB (page index + catalog + DuckDB buffer)

**Repl/CommandParser.cs** - Input parser
- Parses user input into `ParsedCommand` (Built-in or SQL query)
- Built-in commands: `help`, `show tables`, `describe`, `preview`, `load`, `load all`, `exit`
- Everything else treated as SQL passthrough to DuckDB

**Repl/CommandExecutor.cs** - Command executor
- Executes parsed commands using ReplSession
- Handles errors with red console text
- Formats output using TableFormatter

**Repl/QueryResult.cs** - Structured query results
- Success: columns list + rows list
- Error: error message string
- Used to pass results from DuckDbLoader to CommandExecutor

**Repl/TableFormatter.cs** - Pretty-print tables
- Auto-sizes columns based on content (capped at 50 chars)
- Truncates long values with "..."
- Handles NULL values distinctly
- Formats DateTime as "yyyy-MM-dd HH:mm:ss"

### Enhanced DuckDbLoader Methods

Added for REPL support:
- `bool TableExists(string)` - Check if table loaded in DuckDB
- `QueryResult ExecuteQueryWithResults(string)` - Execute SQL, return structured results
- `long GetRowCount(string)` - Get row count for table

### Lazy Loading Strategy

**Key insight**: Loading 2.9M rows takes 60 seconds. Don't load everything upfront.

1. Session starts: Parse MTF, build page index, load catalog (~3 seconds)
2. User queries table: Auto-load on first access (one-time cost per table)
3. Subsequent queries: Use cached data in DuckDB (milliseconds)

Users can explicitly:
- `load <table>` - Load specific table
- `load all` - Load all 71 tables (takes ~60s, useful for batch analysis)

### In-Memory vs Persistent DuckDB

**Default**: `:memory:` database
- Fast startup
- No disk I/O
- Data lost on exit
- Perfect for exploration

**Persistent**: `--db mydata.duckdb`
- Data survives across sessions
- Can resume work later
- Useful for long-running analysis

### Export Capabilities (Leveraging DuckDB)

Users can export directly from REPL using DuckDB's built-in capabilities:

**File exports**:
```sql
COPY Company TO 'company.csv' (HEADER, DELIMITER ',');
COPY Company TO 'company.parquet' (FORMAT PARQUET);
COPY Company TO 'company.json' (FORMAT JSON);
```

**Database exports** (via DuckDB extensions):
```sql
-- PostgreSQL
INSTALL postgres_scanner; LOAD postgres_scanner;
ATTACH 'dbname=mydb user=postgres host=localhost' AS pg (TYPE POSTGRES);
CREATE TABLE pg.company AS SELECT * FROM Company;

-- MySQL
INSTALL mysql; LOAD mysql;
ATTACH 'host=localhost user=root database=mydb' AS mysql (TYPE MYSQL);
CREATE TABLE mysql.company AS SELECT * FROM Company;
```

**No custom export code needed** - DuckDB handles all formats.

### Error Handling

- All command execution wrapped in try/catch
- Errors displayed in red console text
- REPL continues after errors (doesn't exit)
- Table name validation before operations

### Performance Characteristics

- **Session startup**: 2-3 seconds (MTF parse + page index + catalog)
- **Schema commands** (show tables, describe): <10ms (catalog cached)
- **Preview** (first time): Table load time + query (<5s for small tables)
- **Preview** (cached): <10ms
- **SQL queries**: Depends on query complexity
- **Memory usage**: ~30-50MB per session

### Testing the REPL

```bash
# Launch interactive mode
dotnet run -- "Z:\SQLRestore\axs_db_202602040915.BAK" -i

# Test commands
show tables              # Lists all 71 tables
describe Company         # Shows 32 columns with types
preview Company 5        # Auto-loads, shows 5 rows
SELECT COUNT(*) FROM Company  # Returns 11,245
COPY Company TO 'test.csv' (HEADER)  # Exports to CSV
exit                     # Clean shutdown
```

### Future Enhancements (Not Implemented)

- Command history (readline-style)
- Tab completion for table names
- Multi-line SQL input (semicolon-terminated)
- Syntax highlighting
- Background table loading
- Compare mode (diff two backups)
- Script execution (.sql files)
