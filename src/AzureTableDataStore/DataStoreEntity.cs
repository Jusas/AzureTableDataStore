using System;

namespace AzureTableDataStore
{
    /// <summary>
    /// Class representing a data entity stored in Table Storage: it holds the additional fields <see cref="DataStoreEntity{TData}.ETag"/> and <see cref="DataStoreEntity{TData}.Timestamp"/>.
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    public class DataStoreEntity<TData> where TData:new()
    {
        /// <summary>
        /// The retrieved ETag of the entity.
        /// </summary>
        public string ETag { get; set; }

        /// <summary>
        /// The row's timestamp in the Table.
        /// </summary>
        public DateTimeOffset? Timestamp { get; internal set; }

        /// <summary>
        /// The row data as an object.
        /// </summary>
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