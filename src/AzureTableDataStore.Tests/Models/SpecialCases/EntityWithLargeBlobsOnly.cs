namespace AzureTableDataStore.Tests.Models.SpecialCases
{
    public class EntityWithLargeBlobsOnly
    {

        public class BlobContainer
        {
            public LargeBlob BlobA { get; set; }
            public LargeBlob BlobB { get; set; }
        }

        [TableRowKey]
        public string EntityKey { get; set; }

        [TablePartitionKey]
        public string PartitionKey { get; set; }

        public BlobContainer Blobs { get; set; }


    }
}