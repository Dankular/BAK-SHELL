using System.Buffers.Binary;

namespace BakShell.Mdf;

public class LobPointer
{
    public uint Timestamp { get; }
    public RecordPointer Ptr { get; }

    public LobPointer(uint timestamp, RecordPointer ptr)
    {
        Timestamp = timestamp;
        Ptr = ptr;
    }

    public static LobPointer Parse(ReadOnlySpan<byte> data)
    {
        uint timestamp = BinaryPrimitives.ReadUInt32LittleEndian(data[0..4]);
        var ptr = RecordPointer.Parse(data[8..16])
            ?? throw new InvalidDataException("Invalid LOB record pointer");
        return new LobPointer(timestamp, ptr);
    }

    public byte[]? Read(IPageProvider pageProvider)
    {
        var record = pageProvider.GetRecord(Ptr);
        if (record == null) return null;

        var entry = LobEntry.Parse(record);
        if (entry == null) return null;

        var dataBlocks = new List<byte[]>();
        var entries = new List<LobEntry> { entry };

        while (entries.Count > 0)
        {
            var newEntries = new List<LobEntry>();
            foreach (var e in entries)
            {
                if (e.Type is LobType.SmallRoot or LobType.Data)
                {
                    dataBlocks.Add(e.InlineData!);
                }
                else
                {
                    foreach (var sub in e.SubEntries(pageProvider))
                    {
                        if (sub == null) continue;
                        if (sub.Type is LobType.SmallRoot or LobType.Data)
                            dataBlocks.Add(sub.InlineData!);
                        else
                            newEntries.Add(sub);
                    }
                }
            }
            entries = newEntries;
        }

        int totalLen = 0;
        foreach (var b in dataBlocks) totalLen += b.Length;
        var result = new byte[totalLen];
        int pos = 0;
        foreach (var b in dataBlocks)
        {
            b.CopyTo(result, pos);
            pos += b.Length;
        }
        return result;
    }
}

public enum LobType { SmallRoot = 0, Internal = 2, Data = 3, LargeRootYukon = 5, Null = 8 }

public class LobEntry
{
    public LobType Type { get; }
    public byte[]? InlineData { get; }
    public ushort CurLinks { get; }
    private readonly Record? _record;

    private LobEntry(LobType type, byte[]? inlineData = null, ushort curLinks = 0, Record? record = null)
    {
        Type = type;
        InlineData = inlineData;
        CurLinks = curLinks;
        _record = record;
    }

    public static LobEntry? Parse(Record record)
    {
        if (record.FixedData.Length < 10) return null;
        var fixedSpan = record.FixedData.Span;
        ushort ty = BinaryPrimitives.ReadUInt16LittleEndian(fixedSpan[8..10]);

        return ty switch
        {
            0 => ParseSmallRoot(record),
            2 => ParseInternal(record),
            3 => ParseData(record),
            5 => ParseLargeRootYukon(record),
            8 => null, // Null type
            _ => null, // Unknown
        };
    }

    private static LobEntry? ParseSmallRoot(Record record)
    {
        var span = record.FixedData.Span;
        if (span.Length < 16) return null;
        ushort length = BinaryPrimitives.ReadUInt16LittleEndian(span[10..12]);
        int end = Math.Min(16 + length, span.Length);
        return new LobEntry(LobType.SmallRoot, inlineData: span[16..end].ToArray());
    }

    private static LobEntry? ParseData(Record record)
    {
        var span = record.FixedData.Span;
        return new LobEntry(LobType.Data, inlineData: span[10..].ToArray());
    }

    private static LobEntry? ParseLargeRootYukon(Record record)
    {
        var span = record.FixedData.Span;
        if (span.Length < 16) return null;
        ushort curLinks = BinaryPrimitives.ReadUInt16LittleEndian(span[12..14]);
        return new LobEntry(LobType.LargeRootYukon, curLinks: curLinks, record: record);
    }

    private static LobEntry? ParseInternal(Record record)
    {
        var span = record.FixedData.Span;
        if (span.Length < 16) return null;
        ushort curLinks = BinaryPrimitives.ReadUInt16LittleEndian(span[12..14]);
        return new LobEntry(LobType.Internal, curLinks: curLinks, record: record);
    }

    public IEnumerable<LobEntry?> SubEntries(IPageProvider pageProvider)
    {
        for (ushort i = 0; i < CurLinks; i++)
        {
            RecordPointer? ptr = null;
            var fixedSpan = _record!.FixedData.Span;

            if (Type == LobType.LargeRootYukon)
            {
                // SizedRecordPointer: 4 bytes size + 8 bytes RecordPointer, starting at offset 20
                int off = 20 + 12 * i;
                if (off + 12 > fixedSpan.Length) break;
                ptr = RecordPointer.Parse(fixedSpan[(off + 4)..(off + 12)]);
            }
            else if (Type == LobType.Internal)
            {
                // RecordPointerWithOffset: 8 bytes offset + 8 bytes RecordPointer, at 16*(i+1)
                int off = 16 * (i + 1);
                if (off + 16 > fixedSpan.Length) break;
                ptr = RecordPointer.Parse(fixedSpan[(off + 8)..(off + 16)]);
            }

            if (ptr == null) { yield return null; continue; }

            var rec = pageProvider.GetRecord(ptr.Value);
            if (rec == null) { yield return null; continue; }

            yield return Parse(rec);
        }
    }
}
