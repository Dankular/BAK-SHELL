# BAK-SHELL

**Recover and restore SQL Server backups without SQL Server installed.**

BAK-SHELL is a powerful data recovery tool that reads SQL Server `.BAK` backup files directly and enables you to:
- **Restore to any database** - PostgreSQL, MySQL, SQL Server, or any DuckDB-compatible target
- **Export to standard formats** - CSV, JSON, Parquet for use in any analytics tool
- **Explore interactively** - Query and inspect backup data without extraction
- **Emergency data recovery** - Extract critical data when SQL Server is unavailable

## Quick Start

```bash
# Interactive mode - explore and recover data
dotnet run -- backup.bak -i

bak2duckdb> show tables              # List all tables in backup
bak2duckdb> describe Company         # View schema
bak2duckdb> preview Company 10       # Preview data
bak2duckdb> SELECT COUNT(*) FROM Company WHERE Country = 'USA'
bak2duckdb> COPY Company TO 'company.csv' (HEADER)  # Export to CSV
bak2duckdb> exit

# Batch mode - extract all tables
dotnet run -- backup.bak -o output.duckdb
```

## Data Recovery & Restoration Methods

### Restore to SQL Server

```sql
-- From interactive mode: Export to CSV, then use BULK INSERT
bak2duckdb> COPY Company TO 'company.csv' (HEADER, DELIMITER ',');
```

Then in SQL Server:
```sql
BULK INSERT Company
FROM 'C:\path\company.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n');
```

### Restore to PostgreSQL

```sql
-- Install PostgreSQL extension
bak2duckdb> INSTALL postgres_scanner;
bak2duckdb> LOAD postgres_scanner;

-- Connect to target database
bak2duckdb> ATTACH 'dbname=mydb user=postgres host=localhost password=secret' AS pg (TYPE POSTGRES);

-- Restore tables directly
bak2duckdb> CREATE TABLE pg.company AS SELECT * FROM Company;
bak2duckdb> CREATE TABLE pg.orders AS SELECT * FROM Orders;
```

### Restore to MySQL

```sql
-- Install MySQL extension
bak2duckdb> INSTALL mysql;
bak2duckdb> LOAD mysql;

-- Connect to target database
bak2duckdb> ATTACH 'host=localhost user=root database=mydb password=secret' AS mysql (TYPE MYSQL);

-- Restore tables
bak2duckdb> CREATE TABLE mysql.company AS SELECT * FROM Company;
```

### Export for Analytics Tools

```sql
-- Export to CSV (Excel, Tableau, Power BI)
bak2duckdb> COPY Company TO 'company.csv' (HEADER, DELIMITER ',');

-- Export to Parquet (Spark, pandas, Arrow)
bak2duckdb> COPY Company TO 'company.parquet' (FORMAT PARQUET);

-- Export to JSON (MongoDB, Elasticsearch)
bak2duckdb> COPY Company TO 'company.json' (FORMAT JSON, ARRAY true);
```

### Emergency Data Recovery Scenarios

**Scenario 1: SQL Server crashed, need specific table urgently**
```bash
dotnet run -- backup.bak -i
bak2duckdb> preview CriticalTable 100  # Quick preview
bak2duckdb> COPY CriticalTable TO 'emergency_backup.csv' (HEADER);
```

**Scenario 2: Recover deleted records from old backup**
```bash
dotnet run -- old_backup.bak -i
bak2duckdb> SELECT * FROM Customers WHERE DeletedDate IS NULL;
bak2duckdb> COPY (SELECT * FROM Customers WHERE DeletedDate IS NULL) TO 'recovered_customers.csv';
```

**Scenario 3: Migrate to cloud database (PostgreSQL on AWS RDS)**
```bash
dotnet run -- onprem_backup.bak -i
bak2duckdb> INSTALL postgres_scanner; LOAD postgres_scanner;
bak2duckdb> ATTACH 'dbname=prod user=admin host=rds.amazonaws.com password=xxx' AS aws (TYPE POSTGRES);
bak2duckdb> CREATE TABLE aws.customers AS SELECT * FROM Customers;
```

**Scenario 4: Data audit - compare production vs backup**
```bash
dotnet run -- backup.bak -i --db audit.duckdb
# Run queries to compare data states, identify changes
bak2duckdb> SELECT COUNT(*) as backup_count FROM Customers;
# Compare with production database
```

## Features

- **Interactive REPL** - Explore backups interactively without re-parsing
- **No SQL Server required** - Reads `.BAK` files directly using MTF (Microsoft Tape Format) parsing
- **Fast extraction** - ~50K rows/second using DuckDB's Appender API
- **Schema inspection** - View table schemas, column types, and row counts
- **SQL queries** - Run any DuckDB SQL against backup data
- **Export anywhere** - CSV, JSON, Parquet, PostgreSQL, MySQL (via DuckDB extensions)
- **Cross-platform** - .NET 8.0 (works on Windows, Linux, macOS)

## Installation

### Prerequisites

- [.NET 8.0 SDK](https://dotnet.microsoft.com/download/dotnet/8.0)

### Build

```bash
cd BAK-SHELL
dotnet build -c Release
```

### Run

```bash
dotnet run -- <backup.bak> [options]
```

Or build a standalone executable:

```bash
dotnet publish -c Release -r win-x64 --self-contained
```

## Usage

### Interactive Mode (Recommended)

Launch an interactive REPL to explore your backup:

```bash
dotnet run -- backup.bak -i
```

Interactive commands:

```sql
bak2duckdb> show tables              -- List all tables in backup
bak2duckdb> describe Company         -- View table schema
bak2duckdb> preview Company 10       -- Preview first 10 rows
bak2duckdb> SELECT COUNT(*) FROM Company  -- Run SQL queries
bak2duckdb> COPY Company TO 'company.csv' (HEADER)  -- Export to CSV
bak2duckdb> exit                     -- Exit shell
```

The backup is parsed once at startup (~3 seconds), then you can run unlimited queries without re-parsing.

**Persist data across sessions:**
```bash
dotnet run -- backup.bak -i --db mydata.duckdb
```

### Batch Mode

### List tables in a backup

```bash
dotnet run -- backup.bak --list
```

Output:
```
Type            Table                                    Columns
--------------- ---------------------------------------- ----------
UserTable       Company                                          15
UserTable       Movement                                         42
SystemTable     sysallocunits                                     7
...
```

### Extract all user tables

```bash
dotnet run -- backup.bak -o output.duckdb
```

### Extract specific tables

```bash
dotnet run -- backup.bak -t Company,Movement -o data.duckdb
```

### Extract and query

```bash
dotnet run -- backup.bak -o data.duckdb -q "SELECT COUNT(*) FROM Company"
```

### Command-line options

```
Usage: BAK-SHELL <backup.bak> [options]

Modes:
  -i, --interactive     Launch interactive REPL (recommended)
  (default)             Batch extraction mode

Interactive Options:
  --db <file>           DuckDB file for session (default: in-memory)

Batch Options:
  -o, --output <file>   Output DuckDB file (default: <backup>.duckdb)
  -t, --table <names>   Only extract specific table(s), comma-separated
  --list                List tables without extracting
  -q, --query <sql>     Run SQL query after loading

General:
  -h, --help            Show this help
```

## Performance

Tested on a 1.3GB backup file with 71 tables (2.9M rows):

- **Extraction time**: ~60 seconds
- **Throughput**: ~49K rows/second
- **Memory usage**: Low (uses memory-mapped files)

> **Tip**: For best performance, copy `.BAK` files from network shares to a local drive before extraction.

## How It Works

1. **MTF Parser** - Scans the backup file to locate the MQDA stream (database pages)
2. **Page Indexer** - Builds an index of all 8KB MDF pages by (fileId, pageId)
3. **System Tables** - Parses boot page and system tables to discover schema
4. **Row Extractor** - Iterates through data pages and parses records
5. **DuckDB Loader** - Creates tables and bulk loads data using the Appender API

## Architecture

```
.BAK file (MTF format)
  └─ MQDA stream (MDF pages)
      └─ Boot page → System tables → User tables
          └─ Data pages → Records → DuckDB
```

### Key Components

- `Mtf/` - MTF format parser and page provider
  - `MtfParser.cs` - Scans backup file for MQDA stream
  - `MtfPageProvider.cs` - Memory-mapped page access
- `Mdf/` - SQL Server MDF format parser
  - `Database.cs` - Boot page and system table reader
  - `RawPage.cs` - 8KB page parser
  - `Record.cs` - Record-level data extraction
  - `SqlType.cs` - Type system and value parsing
- `DuckDb/` - DuckDB output
  - `DuckDbLoader.cs` - Table creation and bulk loading
  - `TypeMapping.cs` - SQL Server → DuckDB type conversion

## Type Mapping

| SQL Server Type | DuckDB Type | Notes |
|----------------|-------------|-------|
| INT, BIGINT | INTEGER, BIGINT | Direct mapping |
| VARCHAR, NVARCHAR | VARCHAR | UTF-8 encoded |
| DATETIME, DATETIME2 | TIMESTAMP | Preserves precision |
| UNIQUEIDENTIFIER | VARCHAR | UUID format |
| VARBINARY, IMAGE | BLOB | Binary data |
| BIT | BOOLEAN | True/False |

## Limitations

- **Read-only** - This tool only reads backups, it cannot create or modify them
- **User tables only** - System tables are used for metadata but not extracted by default
- **No FILESTREAM data** - External FILESTREAM data is not included in `.BAK` files
- **Computed columns** - Computed columns are skipped (no stored data)

## Dependencies

- **DuckDB.NET.Data.Full** (v1.4.3) - DuckDB database engine for .NET
- **System.Text.Encoding.CodePages** (v8.0.0) - Windows-1252 encoding support

Both are installed automatically via NuGet during build.

## Troubleshooting

### "No database stream (MQDA) found"

The file is not a valid SQL Server `.BAK` file or uses an unsupported format.

### "Cannot read boot page"

The backup file may be corrupted or from an unsupported SQL Server version.

### Slow extraction on network shares

Copy the `.BAK` file to a local drive first:

```bash
copy \\server\share\backup.bak C:\temp\backup.bak
dotnet run -- C:\temp\backup.bak
```

## License

This project is provided as-is for internal use.

## Acknowledgments

This implementation is based on the [mdf-rs](https://github.com/mrd0ll4r/mdf-rs) Rust library, which provided the foundation for understanding SQL Server's internal formats.
