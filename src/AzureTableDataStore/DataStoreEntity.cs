using System;

namespace AzureTableDataStore
{
    public class DataStoreEntity<TData> where TData:new()
    {
        public string ETag { get; set; }
        public DateTimeOffset? Timestamp { get; internal set; }
        public TData Value { get; set; }
    }
}