using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Newtonsoft.Json;

namespace AzureTableDataStore
{
    public sealed class StoredBlob
    {
        [JsonProperty]
        public string Filename { get; internal set; } = "";
        
        [JsonIgnore]
        public Lazy<Task<Stream>> AsyncDataStream { get; internal set; } = null;
        
        [JsonProperty]
        public long Length { get; internal set; } = 0;

        [JsonProperty]
        public string ContentType { get; internal set; } = "";

        public string GetUrl(bool withSasToken = false, TimeSpan tokenExpiration = default)
        {
            // Todo. Requires a reference to the actual storage blob in order to be able to get the URL
            return "";
        }

        public StoredBlob()
        {
        }

        public StoredBlob(string filename, Stream data)
        {
            Filename = filename;
            Length = data.Length;
            AsyncDataStream = new Lazy<Task<Stream>>(() => Task.FromResult(data));
        }

        public StoredBlob(string filename, Func<Task<Stream>> dataFactory)
        {
            Filename = filename;
            Length = 0;
            AsyncDataStream = new Lazy<Task<Stream>>(dataFactory);
        }

        internal StoredBlob(BlobClient sourceBlobClient)
        {
            
        }

        //internal async Task PopulateFromBlobClient(BlobClient sourceBlobClient)
        //{
        //    var properties = await sourceBlobClient.GetPropertiesAsync();
        //    Length = properties.Value.ContentLength;
        //    ContentType = properties.Value.ContentType;

        //}

        public static StoredBlob FromStream(Stream stream, string filename = null)
        {
            // todo
            return new StoredBlob();
        }

        public static StoredBlob FromBytes(byte[] bytes, string filename = null)
        {
            // todo
            return new StoredBlob();
        }

        public static StoredBlob FromString(string data, string filename = null)
        {
            // todo
            return new StoredBlob();
        }
    }
}
