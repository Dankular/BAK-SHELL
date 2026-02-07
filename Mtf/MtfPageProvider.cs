using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Linq;
using BakShell.Mdf;

namespace BakShell.Mtf;

public class MtfPageProvider : IPageProvider, IDisposable
{
    private readonly MemoryMappedFile _mmf;
    private readonly long _mqdaOffset;
    private readonly long _mqdaLength;
    private readonly Dictionary<(ushort fileId, uint pageId), long> _pageOffsets = new();

    public List<ushort> FileIds => _pageOffsets.Keys.Select(k => k.fileId).Distinct().ToList();

    public uint NumPages(ushort fileId)
    {
        var pageIds = _pageOffsets.Keys.Where(k => k.fileId == fileId).Select(k => k.pageId);
        return pageIds.Any() ? pageIds.Max() : 0;
    }

    public MtfPageProvider(string filePath, long mqdaOffset, long mqdaLength)
    {
        _mqdaOffset = mqdaOffset;
        _mqdaLength = mqdaLength;
        _mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        BuildPageIndex();
    }

    private void BuildPageIndex()
    {
        using var accessor = _mmf.CreateViewAccessor(_mqdaOffset, _mqdaLength, MemoryMappedFileAccess.Read);

        long physicalOffset = 0;
        const int pageSize = 8192;
        var headerBuf = new byte[96];

        // Pages are sequential 8KB chunks (no headers between pages)
        while (physicalOffset + pageSize <= _mqdaLength)
        {
            if (physicalOffset + 96 > _mqdaLength) break;

            try
            {
                accessor.ReadArray(physicalOffset, headerBuf, 0, 96);

                // Read pageId and fileId from correct offsets per mdf-rs
                uint pageId = BinaryPrimitives.ReadUInt32LittleEndian(headerBuf.AsSpan(32, 4));
                ushort fileId = BinaryPrimitives.ReadUInt16LittleEndian(headerBuf.AsSpan(36, 2));

                if (pageId > 0 && fileId > 0)
                {
                    // Store first occurrence only
                    if (!_pageOffsets.ContainsKey((fileId, pageId)))
                    {
                        _pageOffsets[(fileId, pageId)] = physicalOffset;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading page at offset {physicalOffset}: {ex.Message}");
                break;
            }

            physicalOffset += pageSize;
        }
    }

    public RawPage? GetPage(PagePointer ptr)
    {
        if (!_pageOffsets.TryGetValue((ptr.FileId, ptr.PageId), out long offset))
            return null;

        using var accessor = _mmf.CreateViewAccessor(_mqdaOffset + offset, 8192, MemoryMappedFileAccess.Read);
        var pageData = new byte[8192];
        accessor.ReadArray(0, pageData, 0, 8192);

        return new RawPage(pageData, this);
    }

    public void Dispose()
    {
        _mmf?.Dispose();
    }
}
