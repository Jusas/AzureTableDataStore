using System;

namespace AzureTableDataStore
{
    public class DataStoreEntity<TData> where TData:new()
    {
        public string ETag { get; set; }
        public DateTimeOffset? Timestamp { get; internal set; }
        public TData Value { get; set; }

        public DataStoreEntity()
        {
        }

        public DataStoreEntity(string etag, TData value)
        {
            ETag = etag;
            Value = value;
        }

        public DataStoreEntity(string etag, TData value, DateTimeOffset timestamp)
        {
            ETag = etag;
            Value = value;
            Timestamp = timestamp;
        }
    }
}