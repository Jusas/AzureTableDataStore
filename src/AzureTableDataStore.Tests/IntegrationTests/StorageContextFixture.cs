using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace AzureTableDataStore.Tests.IntegrationTests
{
    public class StorageContextFixture : IDisposable
    {
        public string TableAndContainerName => TableAndContainerNames.First();
        public List<string> TableAndContainerNames { get; } = new List<string>();
        public string ConnectionString { get; }

        public StorageContextFixture()
        {
            var name = "test" + Guid.NewGuid().ToString().Substring(0, 8);
            TableAndContainerNames.Add(name);

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

        public string GetNewTableName()
        {
            var name = "test" + Guid.NewGuid().ToString().Substring(0, 8);
            TableAndContainerNames.Add(name);
            return name;
        }


        public void DeleteTables()
        {
            var cloudStorageAccount = CloudStorageAccount.Parse(ConnectionString);
            var cloudTableClient = cloudStorageAccount.CreateCloudTableClient();

            foreach (var tableAndContainerName in TableAndContainerNames)
            {
                var table = cloudTableClient.GetTableReference(tableAndContainerName);
                table.DeleteIfExists();
            }
        }

        public void DeleteContainer()
        {
            var blobServiceClient = new BlobServiceClient(ConnectionString);
            
            foreach (var tableAndContainerName in TableAndContainerNames)
            {
                var containerClient = blobServiceClient.GetBlobContainerClient(tableAndContainerName);
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