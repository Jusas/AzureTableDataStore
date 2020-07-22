using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using AzureTableDataStore.Tests.Infrastructure;
using AzureTableDataStore.Tests.Models;
using FluentAssertions;
using Moq;
using Xunit;

namespace AzureTableDataStore.Tests.IntegrationTests
{
    public class ErrorAndExceptionBehaviorTests : IClassFixture<StorageContextFixture>
    {
        private StorageContextFixture _fixture;

        public ErrorAndExceptionBehaviorTests(StorageContextFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]
        public async Task T01_Insert_ValidationIssues()
        {
            var testContext = "insertvalidation";
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);
            
            var entities = MockData.TelescopeMockDataGenerator.CreateDataSet(3);
            
            // Strict mode: no blob properties allowed.

            var ex1 = await Assert.ThrowsAsync<AzureTableDataStoreBatchedOperationException<TelescopePackageProduct>>(
                () => store.InsertOrReplaceAsync(BatchingMode.Strict, entities));

            // Should produce a BatchedOperationException with properties properly populated
            ex1.BatchExceptionContexts.Count.Should().Be(1);
            ex1.BatchExceptionContexts[0].BatchEntities.Count.Should().Be(3);

            
            entities[0].ProductId = null;
            entities[1].CategoryId = string.Join("", Enumerable.Range(0, 2000).Select(x => "z"));
            entities[1].ProductId = "Hello\tworld";
            entities[2].AddedToInventory = DateTime.MinValue;
            entities[2].Description = new string(new char[1*1024*1024]);
            
            // Validation: 
            // First entity row key is null
            // Second entity partition id is too long
            // Second entity row id has illegal chars
            // Second entity blob path will become too long
            // Third entity datetime is out of range
            // Third entity description is too long
            // Third entity size in total is too big

            store.UseClientSideValidation = true;
            var ex2 = await Assert.ThrowsAsync<AzureTableDataStoreEntityValidationException<TelescopePackageProduct>>(
                () => store.InsertAsync(BatchingMode.None, entities));

            ex2.EntityValidationErrors.Keys.Count.Should().Be(3);
            ex2.EntityValidationErrors[entities[0]].Count.Should().Be(1);
            ex2.EntityValidationErrors[entities[1]].Count.Should().Be(3);
            ex2.EntityValidationErrors[entities[2]].Count.Should().Be(3);
        }

        [Fact]
        public async Task T02_Insert_OperationExceptions()
        {
            var mockStream = new Mock<Stream>();
            mockStream.Setup(s => s.CanRead).Returns(true);
            mockStream.Setup(s => s.Seek(It.IsAny<long>(), It.IsAny<SeekOrigin>())).Throws(new Exception("BOOM!"));
            mockStream.Setup(s => s.Length).Returns(100);

            var testContext = "insertexceptions1";
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);

            // Rig one blob op to fail.
            var entities1 = MockData.TelescopeMockDataGenerator.CreateDataSet(3);
            entities1[0].MainImage = new LargeBlob("explodes.png", () => mockStream.Object);
            
            // Act
            var ex1 = await Assert.ThrowsAsync<AzureTableDataStoreMultiOperationException<TelescopePackageProduct>>(
                () => store.InsertAsync(BatchingMode.None, entities1));

            // Should have a proper exception
            ex1.SingleOperationExceptions.Count.Should().Be(1);
            ex1.SingleOperationExceptions[0].Entity.Should().Be(entities1[0]);
            ex1.SingleOperationExceptions[0].BlobOperationExceptions.Count.Should().Be(1);

            // Should have succeeded with all table ops, as blob ops are executed afterwards.
            _fixture.AssertTableEntityExists(testContext, entities1[0].CategoryId, entities1[0].ProductId);
            _fixture.AssertTableEntityExists(testContext, entities1[1].CategoryId, entities1[1].ProductId);
            _fixture.AssertTableEntityExists(testContext, entities1[2].CategoryId, entities1[2].ProductId);



            var entities2 = MockData.TelescopeMockDataGenerator.CreateDataSet(3, "test2");
            
            // Again, rig one blob to explode.
            entities2[0].MainImage = new LargeBlob("boom.png", mockStream.Object);
            
            var ex2 = await Assert.ThrowsAsync<AzureTableDataStoreBatchedOperationException<TelescopePackageProduct>>(
                () => store.InsertAsync(BatchingMode.Loose, entities2));

            _fixture.AssertTableEntityExists(testContext, entities2[0].CategoryId, entities2[0].ProductId);
            _fixture.AssertTableEntityExists(testContext, entities2[1].CategoryId, entities2[1].ProductId);
            _fixture.AssertTableEntityExists(testContext, entities2[2].CategoryId, entities2[2].ProductId);

            // Batch of table entities should have succeeded, but a blob operation afterwards should have failed.
            ex2.BatchExceptionContexts.Count.Should().Be(1);
            ex2.BatchExceptionContexts[0].TableOperationException.Should().BeNull();
            ex2.BatchExceptionContexts[0].BlobOperationExceptions.Count.Should().Be(1);

        }

        [Fact]
        public async Task T03_Merge_NonExistingEntities()
        {
            var testContext = "mergevalidation";
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);

            var entities = MockData.TelescopeMockDataGenerator.CreateDataSet(3);

            store.UseClientSideValidation = true;
            // Should fail, since these entities do not exist yet.

            var ex1 = await Assert.ThrowsAsync<AzureTableDataStoreBatchedOperationException<TelescopePackageProduct>>(
                () => store.MergeAsync(BatchingMode.Loose, null, LargeBlobNullBehavior.IgnoreProperty, entities));

            // Should produce a BatchedOperationException with properties properly populated
            ex1.BatchExceptionContexts.Count.Should().Be(1);
            ex1.BatchExceptionContexts[0].BatchEntities.Count.Should().Be(3);

        }

        [Fact]
        public async Task T04_Merge_StrictAndStrongModeWithBlobThrows()
        {
            var testContext = "mergestrict";
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);

            var entities = MockData.TelescopeMockDataGenerator.CreateDataSet(3);
            
            var ex1 = await Assert.ThrowsAsync<AzureTableDataStoreBatchedOperationException<TelescopePackageProduct>>(
                () => store.MergeAsync(BatchingMode.Strict, null, LargeBlobNullBehavior.DeleteBlob, entities));

            var ex2 = await Assert.ThrowsAsync<AzureTableDataStoreBatchedOperationException<TelescopePackageProduct>>(
                () => store.MergeAsync(BatchingMode.Strong, null, LargeBlobNullBehavior.DeleteBlob, entities));


        }
    }
}