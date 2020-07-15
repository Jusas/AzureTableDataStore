using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using AzureTableDataStore.Tests.Models.SpecialCases;
using FluentAssertions;
using Xunit;

namespace AzureTableDataStore.Tests.IntegrationTests
{
    
    public class SpecialCaseTests : IClassFixture<StorageContextFixture>
    {
        private StorageContextFixture _fixture;

        public SpecialCaseTests(StorageContextFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task T01_ConstructIntermediateObjectsWhenRequired()
        {
            var store = _fixture.GetNewTableDataStore<EntityWithLargeBlobsOnly>("lbo");
            
            // EntityWithLargeBlobsOnly.Blobs is an object that only holds blobs, which will force us to create the intermediate object "Blobs". This is mainly what we test here.

            await store.InsertAsync(BatchingMode.None, new EntityWithLargeBlobsOnly()
            {
                EntityKey = "key",
                PartitionKey = "pkey",
                Blobs = new EntityWithLargeBlobsOnly.BlobContainer()
                {
                    BlobA = new LargeBlob("file1.txt", "test", Encoding.UTF8, "text/plain"),
                    BlobB = new LargeBlob("file2.txt", "test", Encoding.UTF8, "text/plain"),
                }
            });

            var retrieved = await store.GetAsync(x => x.EntityKey == "key");

            // Ensure the structure has been populated correctly.

            retrieved.Blobs.Should().NotBeNull();
            retrieved.Blobs.BlobA.Length.Should().Be(4L);
            retrieved.Blobs.BlobB.Length.Should().Be(4L);
            retrieved.Blobs.BlobA.ContentType.Should().Be("text/plain");
            retrieved.Blobs.BlobB.ContentType.Should().Be("text/plain");


        }

    }
}