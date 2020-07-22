using System;
using System.Collections.Generic;
using System.IO;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json.Linq;

namespace AzureTableDataStore.Tests.IntegrationTests
{
    public class StorageContextFixture : IDisposable
    {
        public Dictionary<string, string> TableAndContainerNames { get; } = new Dictionary<string, string>();
        public string ConnectionString { get; }

        public StorageContextFixture()
        {
            if (File.Exists("Properties\\launchSettings.json"))
            {
                var launchSettings = File.ReadAllText("Properties\\launchSettings.json");
                var jObject = JObject.Parse(launchSettings);

                var connectionString =
                    jObject["profiles"]?["AzureTableDataStore.Tests"]?["environmentVariables"]?["TestAzureStorageConnectionString"]?.Value<string>();

                if(connectionString != null)
                    Environment.SetEnvironmentVariable("TestAzureStorageConnectionString", connectionString);
            }

            ConnectionString = Environment.
                GetEnvironmentVariable("TestAzureStorageConnectionString") ?? "UseDevelopmentStorage=true";
        }

        public string CreateTestTableAndContainerNames(string testContext)
        {
            var name = "test" + Guid.NewGuid().ToString().Substring(0, 8);
            TableAndContainerNames.Add(testContext, name);
            return name;
        }

        public string CreateTestTableAndContainerToStorage(string testContext, PublicAccessType publicAccessType)
        {
            var name = "test" + Guid.NewGuid().ToString().Substring(0, 8);
            TableAndContainerNames.Add(testContext, name);
            CreateStorageTable(name);
            CreateStorageContainer(name, publicAccessType);
            return name;
        }

        public TableDataStore<T> GetNewTableDataStore<T>(string testContext, bool createIfNotExists = true, bool isPublic = false) where T : new()
        {
            var tableAndContainerName = TableAndContainerNames.ContainsKey(testContext)
                ? TableAndContainerNames[testContext]
                : CreateTestTableAndContainerNames(testContext);

            return new TableDataStore<T>(ConnectionString, tableAndContainerName, createIfNotExists, tableAndContainerName,
                createIfNotExists, isPublic ? PublicAccessType.Blob : PublicAccessType.None);
        }

        public TableDataStore<T> GetNewTableDataStoreWithStorageCredentials<T>(string testContext, bool createIfNotExists = true, bool isPublic = false) where T : new()
        {
            var tableAndContainerName = TableAndContainerNames.ContainsKey(testContext)
                ? TableAndContainerNames[testContext]
                : CreateTestTableAndContainerNames(testContext);


            var tableCreds = AzureStorageUtils.GetStorageCredentialsFromConnectionString(ConnectionString);
            var blobCreds = AzureStorageUtils.GetStorageSharedKeyCredentialFromConnectionString(ConnectionString);
            var tableUrl = AzureStorageUtils.GetStorageUriFromConnectionString(ConnectionString, AzureStorageUtils.CredentialType.TableStorage);
            var blobUrl = AzureStorageUtils.GetStorageUriFromConnectionString(ConnectionString, AzureStorageUtils.CredentialType.BlobStorage);

            return new TableDataStore<T>(tableCreds, new StorageUri(new Uri(tableUrl)), tableAndContainerName, false,
                blobCreds, new Uri(blobUrl), tableAndContainerName, false, PublicAccessType.None);
        }

        private void CreateStorageContainer(string containerName, PublicAccessType publicAccessType)
        {
            var blobServiceClient = new BlobServiceClient(ConnectionString);
            blobServiceClient.CreateBlobContainer(containerName, publicAccessType);
        }

        private void CreateStorageTable(string tableName)
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(ConnectionString);
            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();
            var table = cloudTableClient.GetTableReference(tableName);
            table.Create();
        }

        public void DeleteTables()
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(ConnectionString);
            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();

            foreach (var tableAndContainerName in TableAndContainerNames)
            {
                var table = cloudTableClient.GetTableReference(tableAndContainerName.Value);
                table.DeleteIfExists();
            }
        }

        public void DeleteContainer()
        {
            var blobServiceClient = new BlobServiceClient(ConnectionString);
            
            foreach (var tableAndContainerName in TableAndContainerNames)
            {
                var containerClient = blobServiceClient.GetBlobContainerClient(tableAndContainerName.Value);
                containerClient.DeleteIfExists();
            }
        }

        public void Dispose()
        {
            DeleteTables();
            DeleteContainer();
        }

        

    }
}