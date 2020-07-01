namespace AzureTableDataStore
{
    internal class TableBlobReference
    {
        
        public string Container { get; set; }
        public string Path { get; set; }
        public long SizeBytes { get; set; }
        public string MimeType { get; set; }

    }
}