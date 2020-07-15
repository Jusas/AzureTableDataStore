﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
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

        public string CreateTestTableAndContainer(string testContext)
        {
            var name = "test" + Guid.NewGuid().ToString().Substring(0, 8);
            TableAndContainerNames.Add(testContext, name);
            return name;
        }

        public TableDataStore<T> GetNewTableDataStore<T>(string testContext) where T : new()
        {
            var tableAndContainerName = TableAndContainerNames.ContainsKey(testContext)
                ? TableAndContainerNames[testContext]
                : CreateTestTableAndContainer(testContext);

            return new TableDataStore<T>(ConnectionString, tableAndContainerName, tableAndContainerName, 
                PublicAccessType.None);
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