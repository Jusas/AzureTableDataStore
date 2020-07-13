using System;
using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace AzureTableDataStore.Tests.Infrastructure
{
    public class BlobStorageAssertions
    {

        private string _connectionString;
        private string _blobContainer;

        public BlobStorageAssertions(string connectionString, string blobContainer)
        {
            _blobContainer = blobContainer;
            _connectionString = connectionString;
        }

        public void BlobDoesNotExist(string blobPath)
        {
            var blobServiceClient = new BlobServiceClient(_connectionString);
            var exists = blobServiceClient.GetBlobContainerClient(_blobContainer)
                .GetBlobClient(blobPath)
                .Exists();

            if(exists.Value)
                throw new Exception($"Blob {blobPath} exists but should not");
        }

        public async Task BlobDoesNotExistAsync(string blobPath)
        {
            var blobServiceClient = new BlobServiceClient(_connectionString);
            var exists = await blobServiceClient.GetBlobContainerClient(_blobContainer)
                .GetBlobClient(blobPath)
                .ExistsAsync();

            if (exists.Value)
                throw new Exception($"Blob {blobPath} exists but should not");
        }

        public void BlobExists(string blobPath)
        {
            var blobServiceClient = new BlobServiceClient(_connectionString);
            var exists = blobServiceClient.GetBlobContainerClient(_blobContainer)
                .GetBlobClient(blobPath)
                .Exists();

            if (!exists.Value)
                throw new Exception($"Blob {blobPath} does not exist");
        }

        public async Task BlobExistsAsync(string blobPath)
        {
            var blobServiceClient = new BlobServiceClient(_connectionString);
            var exists = await blobServiceClient.GetBlobContainerClient(_blobContainer)
                .GetBlobClient(blobPath)
                .ExistsAsync();

            if (!exists.Value)
                throw new Exception($"Blob {blobPath} does not exist");
        }
    }
}