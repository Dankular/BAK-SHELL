using System.Buffers.Binary;

namespace BakShell.Mdf;

[Flags]
public enum RecordTagA : byte
{
    HasNullBitmap = 1 << 0,
    HasVarLengthColumns = 1 << 1,
    HasVersioningTag = 1 << 2,
    HasValidTagB = 1 << 3,
}

public class VarLengthColumns
{
    private readonly ReadOnlyMemory<byte> _data;
    public ushort Count { get; }
    private readonly int _baseOffset;

    public VarLengthColumns(ReadOnlyMemory<byte> data, ushort count, int baseOffset)
    {
        _data = data;
        Count = count;
        _baseOffset = baseOffset;
    }

    public (bool Complex, ReadOnlyMemory<byte> Data) Get(ushort idx)
    {
        if (idx >= Count)
            return (false, ReadOnlyMemory<byte>.Empty);

        var span = _data.Span;
        int start;
        if (idx == 0)
        {
            start = 2 * Count;
        }
        else
        {
            int prevIdx = idx - 1;
            ushort prevVal = BinaryPrimitives.ReadUInt16LittleEndian(span[(2 * prevIdx)..]);
            start = (prevVal & 0x7FFF) - _baseOffset;
        }

        ushort endVal = BinaryPrimitives.ReadUInt16LittleEndian(span[(2 * idx)..]);
        bool complex = (endVal & 0x8000) != 0;
        int end = (endVal & 0x7FFF) - _baseOffset;

        if (start < 0 || end < start || end > _data.Length)
            return (complex, ReadOnlyMemory<byte>.Empty);

        return (complex, _data[start..end]);
    }
}

public class Record
{
    public RecordTagA TagA { get; }
    public ushort ColumnCount { get; }
    public ReadOnlyMemory<byte> FixedData { get; }
    private readonly ReadOnlyMemory<byte> _nullBitmap;
    public VarLengthColumns? VarColumns { get; }

    private Record(RecordTagA tagA, ushort columnCount, ReadOnlyMemory<byte> fixedData,
        ReadOnlyMemory<byte> nullBitmap, VarLengthColumns? varColumns)
    {
        TagA = tagA;
        ColumnCount = columnCount;
        FixedData = fixedData;
        _nullBitmap = nullBitmap;
        VarColumns = varColumns;
    }

    public bool IsColumnNull(int idx)
    {
        if (_nullBitmap.IsEmpty) return false;
        if (idx >= _nullBitmap.Length * 8) return false;
        int byteIdx = idx / 8;
        int bitIdx = idx % 8;
        return (_nullBitmap.Span[byteIdx] & (1 << bitIdx)) != 0;
    }

    public static Record? Parse(ReadOnlyMemory<byte> data, bool isIndex, ushort pMinLen)
    {
        var span = data.Span;
        if (span.Length < 4) return null;

        var tagA = (RecordTagA)(span[0] >> 4);
        int recordType = (span[0] & 0x0F) >> 1;

        // Only support Primary(0), Index(3), Blob(4)
        if (recordType != 0 && recordType != 3 && recordType != 4)
            return null;

        int fixedDataLength;
        int offset;
        if (isIndex)
        {
            fixedDataLength = pMinLen - 1;
            offset = pMinLen;
        }
        else
        {
            ushort offs = BinaryPrimitives.ReadUInt16LittleEndian(span[2..4]);
            if (offs < 4) return null;
            fixedDataLength = offs - 4;
            offset = 4 + fixedDataLength;
        }

        if (offset > span.Length) return null;

        ushort columnCount = BinaryPrimitives.ReadUInt16LittleEndian(span[offset..]);
        offset += 2;

        ReadOnlyMemory<byte> nullBitmap = ReadOnlyMemory<byte>.Empty;
        if (tagA.HasFlag(RecordTagA.HasNullBitmap))
        {
            int nullBitmapBytes = (columnCount + 7) / 8;
            if (offset + nullBitmapBytes <= span.Length)
            {
                nullBitmap = data[offset..(offset + nullBitmapBytes)];
                offset += nullBitmapBytes;
            }
        }

        VarLengthColumns? varColumns = null;
        if (tagA.HasFlag(RecordTagA.HasVarLengthColumns) && offset + 2 <= span.Length)
        {
            ushort varCount = BinaryPrimitives.ReadUInt16LittleEndian(span[offset..]);
            int varDataStart = offset + 2;
            varColumns = new VarLengthColumns(data[varDataStart..], varCount, varDataStart);
        }

        var fixedData = data[4..(4 + fixedDataLength)];

        return new Record(tagA, columnCount, fixedData, nullBitmap, varColumns);
    }
}
