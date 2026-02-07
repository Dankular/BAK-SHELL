using System.Buffers.Binary;
using System.Text;

namespace BakShell.Mtf;

public class MtfParser : IDisposable
{
    private readonly FileStream _stream;
    private readonly long _length;

    public string FilePath { get; }

    public MtfParser(string filePath)
    {
        FilePath = filePath;
        _stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        _length = _stream.Length;
    }

    /// <summary>
    /// Scans the MTF file to find the MQDA (database) stream.
    /// Returns the byte offset and length of the MQDA data within the file.
    /// </summary>
    public (long offset, long length)? FindMqdaStream()
    {
        _stream.Seek(0, SeekOrigin.Begin);
        var reader = new BinaryReader(_stream, Encoding.ASCII, leaveOpen: true);
        var headerBuf = new byte[52];
        var streamBuf = new byte[22];

        while (_stream.Position + 52 < _length)
        {
            long dblkPos = _stream.Position;

            // Read 52-byte common block header
            if (_stream.Read(headerBuf, 0, 52) < 52) break;

            // Accept any block type - use checksum validation only
            // MTF files can contain Microsoft-specific extensions like MSCI, MQDA

            // Verify checksum
            if (!VerifyChecksum(headerBuf)) break;

            ushort offsetToFirstEvent = BinaryPrimitives.ReadUInt16LittleEndian(headerBuf.AsSpan(8, 2));

            // Jump to first stream
            long streamPos = dblkPos + offsetToFirstEvent;
            if (streamPos >= _length) break;
            _stream.Seek(streamPos, SeekOrigin.Begin);

            // Parse streams
            while (_stream.Position + 22 < _length)
            {
                long streamStart = _stream.Position;

                if (_stream.Read(streamBuf, 0, 22) < 22) break;

                // Check if this is actually a DBLK (not a stream)
                uint streamTypeCheck = BinaryPrimitives.ReadUInt32LittleEndian(streamBuf);
                if (IsKnownDblkType(streamTypeCheck))
                {
                    // Back up — this is the next DBLK, not a stream
                    _stream.Seek(streamStart, SeekOrigin.Begin);
                    break;
                }

                // Verify stream checksum
                if (!VerifyChecksum(streamBuf))
                {
                    _stream.Seek(streamStart, SeekOrigin.Begin);
                    break;
                }

                string streamId = Encoding.ASCII.GetString(streamBuf, 0, 4);
                long dataLength = (long)BinaryPrimitives.ReadUInt64LittleEndian(streamBuf.AsSpan(8, 8));
                long dataStart = _stream.Position;

                // Check if this is MQDA
                if (streamId == "MQDA")
                {
                    // Skip first 2 bytes of MQDA stream (undocumented padding)
                    return (dataStart + 2, dataLength - 2);
                }

                // Skip to next stream (with 4-byte alignment)
                long nextPos = dataStart + dataLength;
                long leftOver = nextPos % 4;
                if (leftOver > 0) nextPos += 4 - leftOver;
                if (nextPos <= streamStart) break;

                _stream.Seek(nextPos, SeekOrigin.Begin);

                if (streamId == "SPAD") break;
            }
        }

        return null;
    }

    private static bool IsKnownDblkType(uint typeId)
    {
        return typeId is 0x45504154  // TAPE
            or 0x54455353  // SSET
            or 0x424C4F56  // VOLB
            or 0x42524944  // DIRB
            or 0x454C4946  // FILE
            or 0x4C494643  // CFIL
            or 0x42505345  // ESPB
            or 0x54455345  // ESET
            or 0x4D544F45  // EOTM
            or 0x424D4653; // SFMB
    }

    private static bool VerifyChecksum(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4) return false;
        ushort xor = 0;
        for (int i = 0; i < data.Length; i += 2)
        {
            ushort word = (i + 1 < data.Length)
                ? BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(i, 2))
                : data[i];
            xor ^= word;
        }
        return xor == 0;
    }

    public void Dispose()
    {
        _stream.Dispose();
    }
}
