using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using AzureTableDataStore.Tests.Models;
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

        [Fact(/*Skip = "Due to table deletion being slow, do not run in normal test sets"*/)]
        public async Task T02_DeleteTableAndContainer()
        {
            var store = _fixture.GetNewTableDataStore<EntityWithLargeBlobsOnly>("deltest");

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

            // Note: this is not instantaneous. It appears that the table is still found immediately after invoking the delete call.
            // This test is somewhat sketchy; we'll have to delay to give time for the table to really disappear.

            await store.DeleteTableAsync(true);

            await Task.Delay(32000);

            // This should throw, the table is gone.
            await Assert.ThrowsAnyAsync<Exception>(() => store.CountRowsAsync());

        }

        [Fact]
        public async Task T03_InitializeClientWithoutAllowingToCreateStorageResources()
        {
            var testContext = "initializingstorage";
            _fixture.CreateTestTableAndContainerToStorage(testContext, PublicAccessType.None);

            var testEntity = MockData.TelescopeMockDataGenerator.CreateDataSet(1);

            var store = _fixture.GetNewTableDataStoreWithStorageCredentials<TelescopePackageProduct>(testContext, false);
            await store.InsertAsync(BatchingMode.None, testEntity);
            
            // Should not throw.
            var results = await store.ListAsync();

            results.Count.Should().Be(1);

            using (var imageStream = await results[0].MainImage.AsyncDataStream.Value)
            {
                using (var reader = new BinaryReader(imageStream))
                {
                    var dataBytes = reader.ReadBytes((int)results[0].MainImage.Length);
                    dataBytes.Length.Should().Be((int)results[0].MainImage.Length);
                }
            }
        }

        [Fact]
        public async Task T04_EntitySerializationDeserializationTestsE2E()
        {
            var testContext = "entityserializations";

            var testEntity = new TestClass1()
            {
                MyDate = DateTime.UtcNow,
                MyDictionaryOfSubClasses = new Dictionary<string, EntitySubClass>()
                {
                    {"myItem", new EntitySubClass() {MyValue = "foo"}}
                },
                MyEntityId = "test",
                MyEntityPartitionKey = "test",
                MyGuid = Guid.NewGuid(),
                MyInt = 1,
                MyLargeBlob = null,
                MyListOfStrings = new List<string>() {"alpha", "beta"},
                MyLong = 123L,
                MyStringValue = "hello",
                MySubClass = new EntitySubClass()
                {
                    MyValue = "world"
                }
            };

            var store = _fixture.GetNewTableDataStore<TestClass1>(testContext);
            await store.InsertAsync(BatchingMode.None, testEntity);

            var results = await store.GetAsync(x => x.MyEntityId == "test");
            results.MyDictionaryOfSubClasses["myItem"].MyValue.Should().Be("foo");
            results.MyListOfStrings.Count.Should().Be(2);
            results.MySubClass.MyValue.Should().Be("world");
        }

    }
}