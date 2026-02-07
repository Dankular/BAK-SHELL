namespace BakShell.Mdf;

public interface IPageProvider
{
    List<ushort> FileIds { get; }
    uint NumPages(ushort fileId);
    RawPage? GetPage(PagePointer ptr);

    Record? GetRecord(RecordPointer ptr)
    {
        var page = GetPage(ptr.PagePtr);
        return page?.GetRecord(ptr.SlotId);
    }
}
