using System;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;

namespace AzureTableDataStore
{
    /// <summary>
    /// A builder for blob download URIs.
    /// </summary>
    internal class BlobUriBuilder
    {
        private BlobServiceClient _blobServiceClient;
        private StorageSharedKeyCredential _credential;

        public BlobUriBuilder(string connectionString)
        {
            _blobServiceClient = new BlobServiceClient(connectionString);
            _credential = AzureStorageUtils.GetStorageSharedKeyCredentialFromConnectionString(connectionString);
        }

        public BlobUriBuilder(StorageSharedKeyCredential credential, Uri blobStorageServiceUri)
        {
            _blobServiceClient = new BlobServiceClient(blobStorageServiceUri, credential);
            _credential = credential;
        }

        public string GetBlobUrl(BlobClient blobClient, bool withSasToken = false, TimeSpan tokenExpiration = default)
        {
            if(blobClient == null)
                throw new AzureTableDataStoreInternalException("Unable to get URL to blob, the instance has not been initialized with a BlobClient. " +
                    "This method is only available for LargeBlob instances instantiated or updated by TableDataStore.");

            var blobUri = blobClient.Uri.ToString();

            if (withSasToken)
            {
                var blobSasBuilder = new BlobSasBuilder()
                {
                    BlobContainerName = blobClient.BlobContainerName,
                    BlobName = blobClient.Name,
                    StartsOn = DateTimeOffset.UtcNow - TimeSpan.FromMinutes(5),
                    ExpiresOn = DateTimeOffset.UtcNow + tokenExpiration
                };
                blobSasBuilder.SetPermissions(BlobSasPermissions.Read);
                var queryParams = blobSasBuilder.ToSasQueryParameters(_credential);
                blobUri = blobUri + "?" + queryParams.ToString();
            }

            return blobUri;
        }
    }
}