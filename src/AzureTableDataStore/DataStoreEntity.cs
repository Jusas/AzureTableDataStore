using System;

namespace AzureTableDataStore
{
    /// <summary>
    /// Class representing a data entity stored in Table Storage: it holds the additional fields <see cref="DataStoreEntity{TData}.ETag"/> and <see cref="DataStoreEntity{TData}.Timestamp"/>.
    /// </summary>
    /// <typeparam name="TData">The entity type.</typeparam>
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

        /// <summary>
        /// Empty constructor.
        /// </summary>
        public DataStoreEntity()
        {
        }

        /// <summary>
        /// Initializes an entity with the specified ETag.
        /// </summary>
        /// <param name="etag"></param>
        /// <param name="value"></param>
        public DataStoreEntity(string etag, TData value)
        {
            ETag = etag;
            Value = value;
        }

        /// <summary>
        /// Initializes an entity with the specified ETag and Timestamp.
        /// </summary>
        /// <param name="etag"></param>
        /// <param name="value"></param>
        /// <param name="timestamp"></param>
        public DataStoreEntity(string etag, TData value, DateTimeOffset timestamp)
        {
            ETag = etag;
            Value = value;
            Timestamp = timestamp;
        }
    }
}