using System;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos.Table;

namespace AzureTableDataStore.Tests.IntegrationTests
{
    public class StorageContextFixture : IDisposable
    {

        public string TableName { get; }
        public string ContainerName { get; }
        public string ConnectionString { get; }
        
        public StorageContextFixture()
        {
            var name = "test" + Guid.NewGuid().ToString().Substring(0, 8);
            TableName = name;
            ContainerName = name;

            ConnectionString = Environment.
                GetEnvironmentVariable("TestAzureStorageConnectionString") ?? "UseDevelopmentStorage=true";
        }


        public void DeleteTable()
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(ConnectionString);
            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            var table = cloudTableClient.GetTableReference(TableName);
            table.DeleteIfExists();
        }

        public void DeleteContainer()
        {
            var blobServiceClient = new BlobServiceClient(ConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
            containerClient.DeleteIfExists();
        }

        public void Dispose()
        {
            //DeleteTable();
            DeleteContainer();
        }

        

    }
}