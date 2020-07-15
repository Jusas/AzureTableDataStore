using System;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using AzureTableDataStore.Tests.IntegrationTests;

namespace AzureTableDataStore.Tests.Infrastructure
{
    public static class BlobAssertionExtensions
    {
        public static void AssertBlobDoesNotExist(this StorageContextFixture fixture, string testContext, string blobPath)
        {
            var blobServiceClient = new BlobServiceClient(fixture.ConnectionString);
            var exists = blobServiceClient.GetBlobContainerClient(fixture.TableAndContainerNames[testContext])
                .GetBlobClient(blobPath)
                .Exists();

            if (exists.Value)
                throw new Exception($"Blob {blobPath} exists but should not");
        }

        public static void AssertBlobExists(this StorageContextFixture fixture, string testContext, string blobPath)
        {
            var blobServiceClient = new BlobServiceClient(fixture.ConnectionString);
            var exists = blobServiceClient.GetBlobContainerClient(fixture.TableAndContainerNames[testContext])
                .GetBlobClient(blobPath)
                .Exists();

            if (!exists.Value)
                throw new Exception($"Blob {blobPath} does not exist");
        }

        public static async Task AssertBlobDoesNotExistAsync(this StorageContextFixture fixture, string testContext, string blobPath)
        {
            var blobServiceClient = new BlobServiceClient(fixture.ConnectionString);
            var exists = await blobServiceClient.GetBlobContainerClient(fixture.TableAndContainerNames[testContext])
                .GetBlobClient(blobPath)
                .ExistsAsync();

            if (exists.Value)
                throw new Exception($"Blob {blobPath} exists but should not");
        }

        public static async Task AssertBlobExistsAsync(this StorageContextFixture fixture, string testContext, string blobPath)
        {
            var blobServiceClient = new BlobServiceClient(fixture.ConnectionString);
            var exists = await blobServiceClient.GetBlobContainerClient(fixture.TableAndContainerNames[testContext])
                .GetBlobClient(blobPath)
                .ExistsAsync();

            if (!exists.Value)
                throw new Exception($"Blob {blobPath} does not exist");
        }

    }
}