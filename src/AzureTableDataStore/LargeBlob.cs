using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Newtonsoft.Json;

namespace AzureTableDataStore
{
    /// <summary>
    /// How to handle an entity's <see cref="LargeBlob"/> properties that have been set to null.
    /// </summary>
    public enum LargeBlobNullBehavior
    {
        /// <summary>
        /// Ignore the property when the value is null: act like it isn't even a part of the entity.
        /// </summary>
        IgnoreProperty,

        /// <summary>
        /// Setting the property value null actually means removing the blob, i.e. deleting the blob, if one already exists.
        /// </summary>
        DeleteBlob
    }

    /// <summary>
    /// A class representing an Azure Storage Blob reference.
    /// <para>
    /// <see cref="LargeBlob"/> is stored in Tables only as a serialized JSON containing the metadata of the blob: the length, content type and the filename. <br/>
    /// The blob content itself is stored in a Blob Container.
    /// </para>
    /// </summary>
    public sealed class LargeBlob
    {
        /// <summary>
        /// The filename of the blob.
        /// </summary>
        [JsonProperty]
        public string Filename { get; internal set; } = "";
        
        /// <summary>
        /// A lazy accessor to the data stream.
        /// <para>
        /// When retrieved from <see cref="TableDataStore{TData}"/> the retrieved Stream points directly to the
        /// Azure Storage Blob data.
        /// </para>
        /// <para>
        /// When the instance is created by the user, it returns the Stream given in the constructor, or a generated
        /// MemoryStream from the input bytes/string.
        /// </para>
        /// </summary>
        [JsonIgnore]
        public Lazy<Task<Stream>> AsyncDataStream { get; internal set; } = null;

        [JsonIgnore]
        internal BlobUriBuilder BlobUriBuilder { get; set; }

        [JsonIgnore]
        internal BlobClient BlobClient { get; set; }
        
        /// <summary>
        /// The length of the content. Automatically calculated and cached in the Table row.
        /// <para>
        /// Stored/cached in the serialized JSON so that the Azure Storage Blob stream does not need to be accessed in order
        /// to get the length.
        /// </para>
        /// </summary>
        [JsonProperty]
        public long Length { get; internal set; } = 0;

        /// <summary>
        /// Content type (MIME) of the data content.
        /// Defaults to "application/octet-stream" if not provided.
        /// </summary>
        [JsonProperty]
        public string ContentType { get; internal set; } = "";

        /// <summary>
        /// Retrieves the Blob download URL.
        /// <para>
        /// NOTE: This method can only be called after the Entity has been inserted to Table Storage. As long as the
        /// Blob does not exist, this method will throw.
        /// The insert/replace/merge/get/find methods populate the necessary properties for this method to work.
        /// </para>
        /// </summary>
        /// <param name="withSasToken">Get the URL with a SAS token (if the storage is not publicly readable)</param>
        /// <param name="tokenExpiration">How long the SAS token will be valid.</param>
        /// <returns>A URL to the blob.</returns>
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
        public string GetDownloadUrl(bool withSasToken = false, TimeSpan tokenExpiration = default)
        {
            if(BlobClient == null)
                throw new AzureTableDataStoreInternalException("Unable to get URL to blob, the instance has not been initialized with a BlobClient. " +
                    "This method is only available for LargeBlob instances instantiated or updated by TableDataStore, when the blob exists in the blob container.");

            if(BlobUriBuilder == null)
                throw new AzureTableDataStoreInternalException("No BlobUriBuilder associated with this blob.");

            return BlobUriBuilder.GetBlobUrl(BlobClient, withSasToken, tokenExpiration);
        }

        /// <summary>
        /// Empty constructor.
        /// </summary>
        public LargeBlob()
        {
        }

        /// <summary>
        /// Initializes a new blob reference from an existing data Stream.
        /// </summary>
        /// <param name="filename">The file's/blob's filename</param>
        /// <param name="data">The data stream</param>
        /// <param name="contentType">MIME content type of the data. Defaults to "application/octet-stream" if not provided.</param>
        public LargeBlob(string filename, Stream data, string contentType = null)
        {
            Filename = filename;
            Length = data.Length;
            AsyncDataStream = new Lazy<Task<Stream>>(() => Task.FromResult(data));
            ContentType = contentType ?? "application/octet-stream";
        }

        /// <summary>
        /// Initializes a new blob from a byte array of data.
        /// </summary>
        /// <param name="filename">The file's/blob's filename</param>
        /// <param name="data">The data bytes</param>
        /// <param name="contentType">MIME content type of the data. Defaults to "application/octet-stream" if not provided.</param>
        public LargeBlob(string filename, byte[] data, string contentType = null)
        {
            Filename = filename;
            Length = data.LongLength;
            AsyncDataStream = new Lazy<Task<Stream>>(() => Task.FromResult((Stream)new MemoryStream(data)));
            ContentType = contentType ?? "application/octet-stream";
        }

        /// <summary>
        /// Initializes a new blob from a string.
        /// </summary>
        /// <param name="filename">The file's/blob's filename</param>
        /// <param name="data">The string</param>
        /// <param name="encoding">The text encoding to use when storing as blob</param>
        /// <param name="contentType">MIME content type of the data. Defaults to "application/octet-stream" if not provided.</param>
        public LargeBlob(string filename, string data, Encoding encoding, string contentType = null)
        {
            var bytes = encoding.GetBytes(data);
            Length = bytes.LongLength;
            Filename = filename;
            AsyncDataStream = new Lazy<Task<Stream>>(() => Task.FromResult((Stream)new MemoryStream(bytes)));
            ContentType = contentType ?? "application/octet-stream";
        }

        /// <summary>
        /// Initializes a new blob from an async factory method that returns a Stream.
        /// </summary>
        /// <param name="filename">The file's/blob's filename</param>
        /// <param name="dataFactory">A method that returns a <see cref="Task"/>&lt;Stream&gt;</param>
        /// <param name="contentType">MIME content type of the data. Defaults to "application/octet-stream" if not provided.</param>
        public LargeBlob(string filename, Func<Task<Stream>> dataFactory, string contentType = null)
        {
            Filename = filename;
            Length = 0;
            AsyncDataStream = new Lazy<Task<Stream>>(dataFactory);
            ContentType = contentType ?? "application/octet-stream";
        }

        /// <summary>
        /// Initializes a new blob from a factory method that returns a Stream.
        /// </summary>
        /// <param name="filename">The file's/blob's filename</param>
        /// <param name="dataFactory">A method that returns a <see cref="Stream"/></param>
        /// <param name="contentType">MIME content type of the data. Defaults to "application/octet-stream" if not provided.</param>
        public LargeBlob(string filename, Func<Stream> dataFactory, string contentType = null)
        {
            Filename = filename;
            Length = 0;
            AsyncDataStream = new Lazy<Task<Stream>>(() => Task.Run(dataFactory));
            ContentType = contentType ?? "application/octet-stream";
        }

    }
}
