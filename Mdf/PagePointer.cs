using System.Buffers.Binary;

namespace BakShell.Mdf;

public readonly record struct PagePointer(uint PageId, ushort FileId)
{
    public static PagePointer? Parse(ReadOnlySpan<byte> data)
    {
        ushort fileId = BinaryPrimitives.ReadUInt16LittleEndian(data[4..6]);
        if (fileId == 0) return null;
        return new PagePointer(BinaryPrimitives.ReadUInt32LittleEndian(data[0..4]), fileId);
    }
}

public readonly record struct RecordPointer(PagePointer PagePtr, ushort SlotId)
{
    public static RecordPointer? Parse(ReadOnlySpan<byte> data)
    {
        ushort fileId = BinaryPrimitives.ReadUInt16LittleEndian(data[4..6]);
        if (fileId == 0) return null;
        var pagePtr = PagePointer.Parse(data[0..6]);
        if (pagePtr is null) return null;
        return new RecordPointer(pagePtr.Value, BinaryPrimitives.ReadUInt16LittleEndian(data[6..8]));
    }
}
