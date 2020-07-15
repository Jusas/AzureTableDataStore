namespace AzureTableDataStore.Tests.Models
{
    public class VerySimpleObject
    {
        [TableRowKey]
        public string Id { get; set; }
        [TablePartitionKey]
        public string Partition { get; set; }
        public int Value { get; set; }
    }
}