using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace AzureTableDataStore.Tests.Infrastructure
{
    public class TableStorageAssertions
    {
        private string _connectionString;
        private string _tableName;

        public TableStorageAssertions(string connectionString, string tableName)
        {
            _tableName = tableName;
            _connectionString = connectionString;
        }

        private TableResult GetEntity(string partitionKey, string rowKey)
        {
            var a = CloudStorageAccount.Parse(_connectionString);
            var c = a.CreateCloudTableClient();

            var result = c.GetTableReference(_tableName).Execute(TableOperation.Retrieve(partitionKey, rowKey));
            return result;
        }

        private async Task<TableResult> GetEntityAsync(string partitionKey, string rowKey)
        {
            var a = CloudStorageAccount.Parse(_connectionString);
            var c = a.CreateCloudTableClient();

            var result = await c.GetTableReference(_tableName)
                .ExecuteAsync(TableOperation.Retrieve(partitionKey, rowKey));
            return result;
        }

        public void TableEntityExists(string partitionKey, string rowKey)
        {
            var result = GetEntity(partitionKey, rowKey);
            if (result.HttpStatusCode != 200)
                throw new Exception("Entity retrieve did not return 200");
        }

        public async Task TableEntityExistsAsync(string partitionKey, string rowKey)
        {
            var result = await GetEntityAsync(partitionKey, rowKey);
            if (result.HttpStatusCode != 200)
                throw new Exception("Entity retrieve did not return 200");
        }

        public void TableEntityDoesNotExist(string partitionKey, string rowKey)
        {
            var result = GetEntity(partitionKey, rowKey);
            if(result.HttpStatusCode != 404)
                throw new Exception("Retrieve should have returned 404, but did not");
        }

        public async Task TableEntityDoesNotExistAsync(string partitionKey, string rowKey)
        {
            var result = await GetEntityAsync(partitionKey, rowKey);
            if (result.HttpStatusCode != 404)
                throw new Exception("Retrieve should have returned 404, but did not");
        }

    }
}