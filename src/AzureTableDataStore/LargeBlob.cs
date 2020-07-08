using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Newtonsoft.Json;

namespace AzureTableDataStore
{
    public sealed class LargeBlob
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

        public LargeBlob()
        {
        }

        public LargeBlob(string filename, Stream data)
        {
            Filename = filename;
            Length = data.Length;
            AsyncDataStream = new Lazy<Task<Stream>>(() => Task.FromResult(data));
        }

        public LargeBlob(string filename, byte[] data)
        {
            Filename = filename;
            Length = data.LongLength;
            AsyncDataStream = new Lazy<Task<Stream>>(() => Task.FromResult((Stream)new MemoryStream(data)));
        }

        public LargeBlob(string filename, string data, Encoding encoding)
        {
            var bytes = encoding.GetBytes(data);
            Length = bytes.LongLength;
            AsyncDataStream = new Lazy<Task<Stream>>(() => Task.FromResult((Stream)new MemoryStream(bytes)));
        }

        public LargeBlob(string filename, Func<Task<Stream>> dataFactory)
        {
            Filename = filename;
            Length = 0;
            AsyncDataStream = new Lazy<Task<Stream>>(dataFactory);
        }

        public LargeBlob(string filename, Func<Stream> dataFactory)
        {
            Filename = filename;
            Length = 0;
            AsyncDataStream = new Lazy<Task<Stream>>(() => Task.Run(dataFactory));
        }

    }
}
