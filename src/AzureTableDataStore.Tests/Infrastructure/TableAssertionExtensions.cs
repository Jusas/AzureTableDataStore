using System;
using System.Threading.Tasks;
using AzureTableDataStore.Tests.IntegrationTests;
using Microsoft.Azure.Cosmos.Table;

namespace AzureTableDataStore.Tests.Infrastructure
{
    public static class TableAssertionExtensions
    {


        private static TableResult GetEntity(StorageContextFixture fixture, string testContext, string partitionKey, string rowKey)
        {
            var tableName = fixture.TableAndContainerNames[testContext];
            var a = CloudStorageAccount.Parse(fixture.ConnectionString);
            var c = a.CreateCloudTableClient();

            var result = c.GetTableReference(tableName).Execute(TableOperation.Retrieve(partitionKey, rowKey));
            return result;
        }

        private static async Task<TableResult> GetEntityAsync(StorageContextFixture fixture, string testContext, string partitionKey, string rowKey)
        {
            var tableName = fixture.TableAndContainerNames[testContext];
            var a = CloudStorageAccount.Parse(fixture.ConnectionString);
            var c = a.CreateCloudTableClient();

            var result = await c.GetTableReference(tableName)
                .ExecuteAsync(TableOperation.Retrieve(partitionKey, rowKey));
            return result;
        }

        public static void AssertTableEntityExists(this StorageContextFixture fixture, string testContext, string partitionKey, string rowKey)
        {
            var result = GetEntity(fixture, testContext, partitionKey, rowKey);
            if (result.HttpStatusCode != 200)
                throw new Exception("Entity retrieve did not return 200");
        }

        public static async Task AssertTableEntityExistsAsync(this StorageContextFixture fixture, string testContext, string partitionKey, string rowKey)
        {
            var result = await GetEntityAsync(fixture, testContext, partitionKey, rowKey);
            if (result.HttpStatusCode != 200)
                throw new Exception("Entity retrieve did not return 200");
        }

        public static void AssertTableEntityDoesNotExist(this StorageContextFixture fixture, string testContext, string partitionKey, string rowKey)
        {
            var result = GetEntity(fixture, testContext, partitionKey, rowKey);
            if (result.HttpStatusCode != 404)
                throw new Exception("Retrieve should have returned 404, but did not");
        }

        public static async Task AssertTableEntityDoesNotExistAsync(this StorageContextFixture fixture, string testContext, string partitionKey, string rowKey)
        {
            var result = await GetEntityAsync(fixture, testContext, partitionKey, rowKey);
            if (result.HttpStatusCode != 404)
                throw new Exception("Retrieve should have returned 404, but did not");
        }
    }
}