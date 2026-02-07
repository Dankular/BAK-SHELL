using System.Buffers.Binary;

namespace BakShell.Mdf;

public enum PageType : byte
{
    UnAlloc = 0,
    Data = 1,
    Index = 2,
    TextMix = 3,
    TextTree = 4,
    Sort = 7,
    GAM = 8,
    SGAM = 9,
    IAM = 10,
    PFS = 11,
    Boot = 13,
    FileHeader = 15,
    DiffMap = 16,
    MLMap = 17,
}

public class PageHeader
{
    public PagePointer Ptr { get; init; }
    public PageType Type { get; init; }
    public byte Level { get; init; }
    public ushort PMinLen { get; init; }
    public ushort SlotCount { get; init; }
    public uint ObjectId { get; init; }
    public ushort IndexId { get; init; }
    public PagePointer? PrevPagePtr { get; init; }
    public PagePointer? NextPagePtr { get; init; }

    public static PageHeader Parse(ReadOnlySpan<byte> data)
    {
        return new PageHeader
        {
            Ptr = ParsePtr(data)!.Value,
            Type = (PageType)data[1],
            Level = data[3],
            IndexId = BinaryPrimitives.ReadUInt16LittleEndian(data[6..8]),
            PMinLen = BinaryPrimitives.ReadUInt16LittleEndian(data[14..16]),
            SlotCount = BinaryPrimitives.ReadUInt16LittleEndian(data[22..24]),
            ObjectId = BinaryPrimitives.ReadUInt32LittleEndian(data[24..28]),
            PrevPagePtr = PagePointer.Parse(data[8..14]),
            NextPagePtr = PagePointer.Parse(data[16..22]),
        };
    }

    public static PagePointer? ParsePtr(ReadOnlySpan<byte> data)
    {
        return PagePointer.Parse(data[32..]);
    }
}

public class RawPage
{
    public const int PAGE_SIZE = 8192;

    public PageHeader Header { get; }
    public ReadOnlyMemory<byte> Data { get; }
    public IPageProvider PageProvider { get; }

    public RawPage(ReadOnlyMemory<byte> data, IPageProvider pageProvider)
    {
        Header = PageHeader.Parse(data.Span);
        Data = data[..PAGE_SIZE];
        PageProvider = pageProvider;
    }

    public int RecordCount => Header.SlotCount;

    public Record? GetRecord(ushort idx)
    {
        if (idx >= RecordCount) return null;

        int slotArrayPos = PAGE_SIZE - 2 * idx - 2;
        ushort offset = BinaryPrimitives.ReadUInt16LittleEndian(Data.Span[slotArrayPos..]);
        return Record.Parse(
            Data[offset..],
            Header.Type == PageType.Index,
            Header.PMinLen);
    }

    public IEnumerable<Record> Records(bool localOnly = false)
    {
        var currentPage = this;
        while (currentPage != null)
        {
            for (ushort i = 0; i < currentPage.RecordCount; i++)
            {
                var record = currentPage.GetRecord(i);
                if (record != null)
                    yield return record;
            }

            if (localOnly || currentPage.Header.NextPagePtr is null)
                break;

            currentPage = currentPage.PageProvider.GetPage(currentPage.Header.NextPagePtr.Value);
        }
    }
}
