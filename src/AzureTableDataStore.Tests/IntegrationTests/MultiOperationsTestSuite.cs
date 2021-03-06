﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AzureTableDataStore.Tests.Infrastructure;
using AzureTableDataStore.Tests.Models;
using FluentAssertions;
using Microsoft.Azure.Cosmos.Table;
using Moq;
using Xunit;

namespace AzureTableDataStore.Tests.IntegrationTests
{
    [TestCaseOrderer("AzureTableDataStore.Tests.Infrastructure.AlphabeticalTestCaseOrderer", "AzureTableDataStore.Tests")]
    public class MultiOperationsTestSuite : IClassFixture<StorageContextFixture>
    {

        private StorageContextFixture _fixture;

        public MultiOperationsTestSuite(StorageContextFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T01_InsertMultiple_NonBatched_WithBlobs()
        {
            // Arrange

            var testDataSet = MockData.TelescopeMockDataGenerator.SmallDataSet;
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");


            // Act

            await store.InsertAsync(BatchingMode.None, testDataSet.ToArray());

            // Assert

            var tasks = testDataSet.Select(
                x => _fixture.AssertTableEntityExistsAsync("base", x.CategoryId, x.ProductId));
            await Task.WhenAll(tasks);

        }


        [Fact(/*Skip = "reason"*/)]
        public async Task T02_FindMultiple_ShouldReturnFullObjects_WithBlobs()
        {
            // Arrange

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");


            // Act

            var allFullTelescopes = await store.FindAsync(x => x.CategoryId == "telescopes-full");
            var allOtasOnly = await store.FindAsync(x => x.CategoryId == "telescopes-ota");

            // Assert

            allFullTelescopes.Count.Should().Be(3);
            allOtasOnly.Count.Should().Be(1);

            var withImage1 = allFullTelescopes.FirstOrDefault(x => x.ProductId == "omegon-ac-70-700-az2");
            var withImage2 = allFullTelescopes.FirstOrDefault(x => x.ProductId == "meade-telescope-n-2001000-lx85-goto");

            withImage1.Should().NotBeNull();
            withImage2.Should().NotBeNull();

            withImage1.MainImage.Length.Should().Be(22_625L);
            withImage2.MainImage.Length.Should().Be(49_125L);

            using (var imageStream = await withImage1.MainImage.AsyncDataStream.Value)
            {
                using (var reader = new BinaryReader(imageStream))
                {
                    var dataBytes = reader.ReadBytes((int)withImage1.MainImage.Length);
                    dataBytes.Length.Should().Be((int)withImage1.MainImage.Length);
                }
            }
            using (var imageStream = await withImage2.MainImage.AsyncDataStream.Value)
            {
                using (var reader = new BinaryReader(imageStream))
                {
                    var dataBytes = reader.ReadBytes((int)withImage2.MainImage.Length);
                    dataBytes.Length.Should().Be((int)withImage2.MainImage.Length);
                }
            }

            withImage1.Name.Should().Be("Omegon Telescope AC 70/700 AZ-2");
            withImage1.CategoryId.Should().Be("telescopes-full");
            withImage1.ProductId.Should().Be("omegon-ac-70-700-az2");
            withImage1.Description.Should().Be(
                "The Omegon AC 70/700 telescope is your first taste of the world of astronomy. Practical observing with it is so simple that it highly suitable for children and adults alike.\r\n\r\n\r\nThe instrument is simple to understand and is very quick to set up, without any tools being required. Simply set it up, insert eyepiece and observe!");
            withImage1.PackageDepthMm.Should().Be(2200);
            withImage1.PackageHeightMm.Should().Be(500);
            withImage1.PackageWidthMm.Should().Be(500);
            withImage1.SearchNames.Should()
                .ContainInOrder("Omegon Telescope AC 70/700 AZ-2", "Omegon AC 70/700", "Omegon AZ-2");
            withImage1.Specifications.ApplicationDescription.Should().Be("General visual observation of sky and nature");
            withImage1.Specifications.ForAstrophotography.Should().Be(false);
            withImage1.Specifications.ForVisualObservation.Should().Be(true);
            withImage1.Specifications.Mount.Type.Should().Be("AZ-2");
            withImage1.Specifications.Mount.GotoControl.Should().Be(false);
            withImage1.Specifications.Mount.Mounting.Should().Be(MountingType.Azimuthal);
            withImage1.Specifications.Mount.Tracking.Should().Be(false);
            withImage1.Specifications.Optics.Type.Should().Be("Refractor");
            withImage1.Specifications.Optics.ApertureMm.Should().Be(70);
            withImage1.Specifications.Optics.ApertureRatioF.Should().Be(10);
            withImage1.Specifications.Optics.FocalLengthMm.Should().Be(700);
            withImage1.Specifications.Tripod.HeightDescription.Should().Be("66-120mm adjustable");
            withImage1.Specifications.Tripod.Material.Should().Be("Aluminum");
            withImage1.Specifications.Tripod.WeightKg.Should().Be(2);

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T03_MergeMultiple_NonBatched_WithBlobs_IgnoreEtags()
        {
            // Relies on T01 insert.

            // Arrange

            var testDataSet = MockData.TelescopeMockDataGenerator.SmallDataSet;
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            long expectedImgSize;
            using (var newImage = new FileStream("Resources/meade-telescope-n-2001000-lx85-goto.png", FileMode.Open,
                FileAccess.Read))
            {
                expectedImgSize = newImage.Length;
            }

            // Note: here we have decided to ignore null LargeBlobs => the other main images will not be touched in any way.

            testDataSet[0].Name = "Telescope 1";
            testDataSet[0].Description = "Changed";
            testDataSet[0].MainImage = new LargeBlob("newimage.png", () => new FileStream("Resources/meade-telescope-n-2001000-lx85-goto.png", FileMode.Open, FileAccess.Read));
            testDataSet[1].Name = "Telescope 2";
            testDataSet[1].Description = "Changed";
            testDataSet[1].MainImage = null;
            testDataSet[2].Name = "Telescope 3";
            testDataSet[2].Description = "Changed";
            testDataSet[3].Name = "Telescope 4";
            testDataSet[3].Description = "Changed";

            // Act

            await store.MergeAsync(BatchingMode.None, x => new
            {
                x.MainImage,
                x.Name,
                x.Description
            }, LargeBlobNullBehavior.IgnoreProperty, testDataSet.ToArray());

            
            // Assert

            var changedItems = await store.FindAsync(x => x.Description == "Changed");

            changedItems.Count.Should().Be(4);
            var itemWithChangedImage = changedItems.First(x => x.Name == "Telescope 1");

            using (var imageStream = await itemWithChangedImage.MainImage.AsyncDataStream.Value)
            {
                using (var reader = new BinaryReader(imageStream))
                {
                    var dataBytes = reader.ReadBytes((int)itemWithChangedImage.MainImage.Length);
                    dataBytes.Length.Should().Be((int)expectedImgSize);
                }
            }

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T04_MergeMultiple_NonBatched_WithBlobs_UsingDeleteNullBehavior()
        {
            // Relies on T01 insert.

            // Arrange

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");
            var testDataSet = await store.FindAsync((x, dt) => dt < DateTime.UtcNow);

            testDataSet.Count.Should().Be(4);

            // Images for Telescope 1 and Telescope 2 should get deleted, while new ones should be added for 3 and 4.

            var newBlobFor3 = new LargeBlob("newimage.png", () => new FileStream("Resources/meade-telescope-n-2001000-lx85-goto.png", FileMode.Open, FileAccess.Read));
            var newBlobFor4 = new LargeBlob("newimage.png", () => new FileStream("Resources/meade-telescope-n-2001000-lx85-goto.png", FileMode.Open, FileAccess.Read));

            var mainImageFilename1 = testDataSet[0].MainImage.Filename;
            var mainImageFilename2 = testDataSet[1].MainImage.Filename;
            var mainImageFilename3 = newBlobFor3.Filename;
            var mainImageFilename4 = newBlobFor4.Filename;

            var blobPath1 = store.BuildBlobPath(_fixture.TableAndContainerNames["base"], testDataSet[0].CategoryId,
                testDataSet[0].ProductId, "MainImage", mainImageFilename1);
            var blobPath2 = store.BuildBlobPath(_fixture.TableAndContainerNames["base"], testDataSet[1].CategoryId,
                testDataSet[1].ProductId, "MainImage", mainImageFilename2);
            var blobPath3 = store.BuildBlobPath(_fixture.TableAndContainerNames["base"], testDataSet[2].CategoryId,
                testDataSet[2].ProductId, "MainImage", mainImageFilename3);
            var blobPath4 = store.BuildBlobPath(_fixture.TableAndContainerNames["base"], testDataSet[3].CategoryId,
                testDataSet[3].ProductId, "MainImage", mainImageFilename4);

            _fixture.AssertBlobExists("base", blobPath1);
            _fixture.AssertBlobExists("base", blobPath2);
            _fixture.AssertBlobDoesNotExist("base", blobPath3);
            _fixture.AssertBlobDoesNotExist("base", blobPath4);
            
            testDataSet[0].Name = "Telescope 1";
            testDataSet[0].Description = "Changed";
            testDataSet[0].MainImage = null;
            testDataSet[1].Name = "Telescope 2";
            testDataSet[1].Description = "Changed";
            testDataSet[1].MainImage = null;
            testDataSet[2].Name = "Telescope 3";
            testDataSet[2].Description = "Changed";
            testDataSet[2].MainImage = newBlobFor3;
            testDataSet[3].Name = "Telescope 4";
            testDataSet[3].Description = "Changed";
            testDataSet[3].MainImage = newBlobFor4;

            // Act

            await store.MergeAsync(BatchingMode.None, x => new
            {
                x.MainImage,
                x.Name,
                x.Description
            }, LargeBlobNullBehavior.DeleteBlob, testDataSet.ToArray());


            // Assert

            _fixture.AssertBlobExists("base", blobPath3);
            _fixture.AssertBlobExists("base", blobPath4);
            _fixture.AssertBlobDoesNotExist("base", blobPath1);
            _fixture.AssertBlobDoesNotExist("base", blobPath2);
            
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T05_ReplaceMultiple_NonBatched_ShouldDeleteBlobsWhenPropertiesSetToNull()
        {
            // Relies on T01 insert.

            // Arrange

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            var allItems = await store.FindAsync((x, dt) => dt < DateTimeOffset.UtcNow);

            allItems.Count.Should().Be(4);

            // We're gonna set the MainImages to null, and because we're replacing, this should cause the deletion of the blobs already uploaded to storage.

            var blobPath3 = store.BuildBlobPath(_fixture.TableAndContainerNames["base"], allItems[2].CategoryId,
                allItems[2].ProductId, "MainImage", allItems[2].MainImage.Filename);
            var blobPath4 = store.BuildBlobPath(_fixture.TableAndContainerNames["base"], allItems[3].CategoryId,
                allItems[3].ProductId, "MainImage", allItems[3].MainImage.Filename);

            _fixture.AssertBlobExists("base", blobPath3);
            _fixture.AssertBlobExists("base", blobPath4);

            allItems[2].MainImage = null;
            allItems[2].Name = "Replaced 3";
            allItems[3].MainImage = null;
            allItems[3].Name = "Replaced 4";

            await store.InsertOrReplaceAsync(BatchingMode.None, allItems.Skip(2).Take(2).ToArray());

            _fixture.AssertBlobDoesNotExist("base", blobPath3);
            _fixture.AssertBlobDoesNotExist("base", blobPath4);

        }


        [Fact(/*Skip = "reason"*/)]
        public async Task T06_MergeMultiple_StrictBatched_UsingEtags_SelectedProperties()
        {
            // Relies on T01 insert.

            // Arrange
            
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");


            // Act & Assert

            var telescopes = await store.FindWithMetadataAsync(x => x.CategoryId == "telescopes-full", x => new
            {
                x.Name,
                x.Description
            });

            telescopes.Count.Should().Be(3);

            telescopes[0].Value.Name = "Telescope 1a";
            telescopes[1].Value.Name = "Telescope 2a";
            telescopes[2].Value.Name = "Telescope 3a";

            telescopes[0].Value.Description = "Changed 2";
            telescopes[1].Value.Description = "Changed 2";
            telescopes[2].Value.Description = "Changed 2";

            // For Strict mode with entities having LargeBlob properties, LargeBlobNullBehavior must always be set to IgnoreProperty.

            await store.MergeAsync(BatchingMode.Strict, x => new
            {
                x.Name,
                x.Description
            }, LargeBlobNullBehavior.IgnoreProperty, telescopes.ToArray());


            // Assert

            var changedItems = await store.FindWithMetadataAsync(x => x.Description == "Changed 2", x => new
            {
                x.Name
            });

            changedItems.Count.Should().Be(3);
            changedItems.Should().ContainSingle(x => x.Value.Name == "Telescope 1a");
            changedItems.Should().ContainSingle(x => x.Value.Name == "Telescope 2a");
            changedItems.Should().ContainSingle(x => x.Value.Name == "Telescope 3a");

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T07_Find_WithStringComparison()
        {
            // Arrange

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            // All items that come alphabetically before "Omegon"
            var allBeforeO = await store.FindAsync(x => x.Name.AsComparable() < "O".AsComparable());

            Assert.True(allBeforeO.All(x => !x.Name.StartsWith("O")));
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T08_FindWithMetadata()
        {
            // Arrange

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            // All items that are for astrophotography
            var allForAstrophotography = await store.FindWithMetadataAsync(x => x.Specifications.ForAstrophotography == true);

            Assert.True(allForAstrophotography.All(x => x.Value.Specifications.ForAstrophotography == true));
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T09_FindWithMetadata_WithDate()
        {
            // Arrange

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            // All items that are for astrophotography and older than now
            var allForAstrophotography = await store.FindWithMetadataAsync((x, dt) => x.Specifications.ForAstrophotography == true && dt < DateTime.UtcNow);

            Assert.True(allForAstrophotography.All(x => x.Value.Specifications.ForAstrophotography == true));
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T10_InsertHugeBatch_StrongMode_WithoutBlobs()
        {
            // Arrange

            // Set images to null so that we can use the batch insert with this data model.
            var itemsToAdd = MockData.TelescopeMockDataGenerator.CreateDataSet(10_000);
            for (var i = 0; i < itemsToAdd.Length; i++)
            {
                itemsToAdd[i].MainImage = null;
            }

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            // Act

            // 7500 to partition 1, and 2500 to partition 2, with 100 per batch. 100 batches.
            await store.InsertAsync(BatchingMode.Strong, itemsToAdd);

            // Should not throw.
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T11_InsertBatches_WithVeryLargeEntityContent_StrongMode_WithoutBlobs()
        {
            // Arrange

            // Set images to null so that we can use the batch insert with this data model.
            var itemsToAdd = MockData.TelescopeMockDataGenerator.CreateDataSet(100, "largecontent");
            for (var i = 0; i < itemsToAdd.Length; i++)
            {
                itemsToAdd[i].MainImage = null;
            }

            // Make sure the data we're inserting will go over the 4MB per batch rule.
            // Note the one property rule of max 64kb size, 32k chars of UTF-16.

            var longText = new string(Enumerable.Repeat('a', 32000).ToArray());
            for (var i = 0; i < itemsToAdd.Length; i++)
            {
                itemsToAdd[i].Description = longText;
                itemsToAdd[i].Name = longText;
            }

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            // Act

            await store.InsertAsync(BatchingMode.Strong, itemsToAdd);

            // Should not throw, should succeed by splitting the content into multiple batches.
        }

        [Fact(/*Skip = "until exceptions are unified after merging batched and single ops"*/)]
        public async Task T12_InsertBatches_RaisingExceptionsFromValidation_WithoutBlobs()
        {
            // Arrange

            // Set images to null so that we can use the batch insert with this data model.
            var itemsToAdd = MockData.TelescopeMockDataGenerator.CreateDataSet(150, partitionKey: "exceptions1");
            for (var i = 0; i < itemsToAdd.Length; i++)
            {
                itemsToAdd[i].MainImage = null;
            }

            // Make a few entities contain content that cannot be stored.
            var longText = new string(Enumerable.Repeat('a', 64000).ToArray());
            for (var i = 5; i < 8; i++)
            {
                itemsToAdd[i].Description = longText;
                itemsToAdd[i].Name = longText;
            }

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");
            store.UseClientSideValidation = true;

            // Act

            // Client side validation should catch this early.

            var exception = await Assert.ThrowsAsync<AzureTableDataStoreEntityValidationException<TelescopePackageProduct>>(
                () => store.InsertAsync(BatchingMode.Strong, itemsToAdd));

            exception.EntityValidationErrors.Count.Should().Be(3);
            

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T13_InsertBatches_LooseMode_WithBlobs()
        {
            // Arrange

            var itemsToAdd = MockData.TelescopeMockDataGenerator.CreateDataSet(250, partitionKey: "loose1");
            for (var i = 0; i < itemsToAdd.Length; i++)
            {
                itemsToAdd[i].MainImage = new LargeBlob(itemsToAdd[i].ProductId + ".blob", "just some data", Encoding.UTF8, "text/plain");
            }

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");
            store.UseClientSideValidation = true;

            // Act

            await store.InsertAsync(BatchingMode.Loose, itemsToAdd);


            // Assert

            var blobNames = itemsToAdd.Select(x => store.BuildBlobPath(_fixture.TableAndContainerNames["base"],
                x.CategoryId, x.ProductId, "MainImage", x.MainImage.Filename));

            var tasks = blobNames.Select(n => _fixture.AssertBlobExistsAsync("base", n));
            await Task.WhenAll(tasks);

            tasks = itemsToAdd.Select(item => _fixture.AssertTableEntityExistsAsync("base", item.CategoryId, item.ProductId));
            await Task.WhenAll(tasks);
            
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T14_InsertBatches_LooseMode_WithBlobs_SomeUploadsShouldFail()
        {
            // Arrange
            
            // The first 2 items of the first batch will be rigged to throw upon reading the stream content.

            var mockStream = new Mock<Stream>();
            mockStream.Setup(s => s.CanRead).Returns(true);
            mockStream.Setup(s => s.Seek(It.IsAny<long>(), It.IsAny<SeekOrigin>())).Throws(new Exception("BOOM!"));
            mockStream.Setup(s => s.Length).Returns(100);

            var itemsToAdd1 = MockData.TelescopeMockDataGenerator.CreateDataSet(5, partitionKey: "loose2_1");
            var itemsToAdd2 = MockData.TelescopeMockDataGenerator.CreateDataSet(5, partitionKey: "loose2_2");

            for (var i = 0; i < itemsToAdd1.Length; i++)
            {
                if (i < 2)
                    itemsToAdd1[i].MainImage = new LargeBlob(itemsToAdd1[i].ProductId + ".blob", () => mockStream.Object);
                else
                    itemsToAdd1[i].MainImage = new LargeBlob(itemsToAdd1[i].ProductId + ".blob", "just some data", Encoding.UTF8, "text/plain");
            }
            for (var i = 0; i < itemsToAdd2.Length; i++)
            {
                itemsToAdd2[i].MainImage = new LargeBlob(itemsToAdd2[i].ProductId + ".blob", "just some data", Encoding.UTF8, "text/plain");
            }

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");
            store.UseClientSideValidation = true;

            // Act

            var exception = await Assert.ThrowsAsync<AzureTableDataStoreBatchedOperationException<TelescopePackageProduct>>(
                () => store.InsertAsync(BatchingMode.Loose, itemsToAdd1.Concat(itemsToAdd2).ToArray()));

            // Assert

            // One batch should contain errors.
            exception.BatchExceptionContexts.Count.Should().Be(1);
            // BatchEntities should contain the entities in this batch.
            exception.BatchExceptionContexts[0].BatchEntities.Count.Should().Be(5);
            // BlobOperationExceptions should contain 2 failed blob ops.
            exception.BatchExceptionContexts[0].BlobOperationExceptions.Count.Should().Be(2);

            // Expected to see the two exact source blobs fail.
            var fail1 = exception.BatchExceptionContexts[0].BlobOperationExceptions
                .FirstOrDefault(x => x.SourceBlob == itemsToAdd1[0].MainImage);
            fail1.Should().NotBeNull();

            var fail2 = exception.BatchExceptionContexts[0].BlobOperationExceptions
                .FirstOrDefault(x => x.SourceBlob == itemsToAdd1[1].MainImage);
            fail2.Should().NotBeNull();

            fail1.SourceEntity.Should().Be(itemsToAdd1[0]);
            fail2.SourceEntity.Should().Be(itemsToAdd1[1]);


        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T15_InsertOrReplaceBatches_LooseMode_WithBlobs()
        {
            // Arrange

            var itemsToAdd = MockData.TelescopeMockDataGenerator.CreateDataSet(5, partitionKey: "loose3_1");
            var itemsToInsertOrReplace = MockData.TelescopeMockDataGenerator.CreateDataSet(10, partitionKey: "loose3_1");

            for (var i = 0; i < itemsToAdd.Length; i++)
            {
                itemsToAdd[i].MainImage = new LargeBlob(itemsToAdd[i].ProductId + ".blob", "just some data", Encoding.UTF8, "text/plain");
            }
            for (var i = 0; i < itemsToInsertOrReplace.Length; i++)
            {
                itemsToInsertOrReplace[i].InternalReferenceId = Guid.Empty;;
                itemsToInsertOrReplace[i].MainImage = new LargeBlob(itemsToInsertOrReplace[i].ProductId + ".blob", "some replaced data", Encoding.UTF8, "text/plain");
            }

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");
            store.UseClientSideValidation = true;

            // Act

            await store.InsertOrReplaceAsync(BatchingMode.Loose, itemsToAdd);

            await store.InsertOrReplaceAsync(BatchingMode.Loose, itemsToInsertOrReplace);

            var foundEntries = await store.FindAsync(x => x.CategoryId == "loose3_1");

            // Assert

            var tasks = itemsToInsertOrReplace.Select(item => _fixture.AssertTableEntityExistsAsync("base", item.CategoryId, item.ProductId));
            await Task.WhenAll(tasks);

            var blobNames = itemsToInsertOrReplace.Select(x => store.BuildBlobPath(_fixture.TableAndContainerNames["base"],
                x.CategoryId, x.ProductId, "MainImage", x.MainImage.Filename));

            tasks = blobNames.Select(n => _fixture.AssertBlobExistsAsync("base", n));
            await Task.WhenAll(tasks);

            foundEntries.Count.Should().Be(10);
            foundEntries.Select(x => x.MainImage.Length).Should().AllBeEquivalentTo(Encoding.UTF8.GetByteCount("some replaced data"));
            foundEntries.Select(x => x.InternalReferenceId).Should().AllBeEquivalentTo(Guid.Empty);

        }


        [Fact(/*Skip = "reason"*/)]
        public async Task T16_InsertBatches_LooseMode_WithBlobs_ThenDeleteUsingLooseBatching()
        {
            // Arrange

            var itemsToAdd = MockData.TelescopeMockDataGenerator.CreateDataSet(250, partitionKey: "loosedelete1");
            for (var i = 0; i < itemsToAdd.Length; i++)
            {
                itemsToAdd[i].MainImage = new LargeBlob(itemsToAdd[i].ProductId + ".blob", "just some data", Encoding.UTF8, "text/plain");
            }

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");
            store.UseClientSideValidation = true;

            // Act

            await store.InsertAsync(BatchingMode.Loose, itemsToAdd);


            // Assert

            var blobNames = itemsToAdd.Select(x => store.BuildBlobPath(_fixture.TableAndContainerNames["base"],
                x.CategoryId, x.ProductId, "MainImage", x.MainImage.Filename));

            var tasks = blobNames.Select(n => _fixture.AssertBlobExistsAsync("base", n));
            await Task.WhenAll(tasks);

            tasks = itemsToAdd.Select(item => _fixture.AssertTableEntityExistsAsync("base", item.CategoryId, item.ProductId));
            await Task.WhenAll(tasks);

            var itemsToDelete = itemsToAdd.Select(x => (x.CategoryId, x.ProductId)).ToArray();

            await store.DeleteAsync(BatchingMode.Loose, itemsToDelete);

            tasks = blobNames.Select(n => _fixture.AssertBlobDoesNotExistAsync("base", n));
            await Task.WhenAll(tasks);

            tasks = itemsToAdd.Select(item => _fixture.AssertTableEntityDoesNotExistAsync("base", item.CategoryId, item.ProductId));
            await Task.WhenAll(tasks);

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T17_InsertBatches_LooseMode_WithBlobs_ThenTryDeleteUsingStrongBatching_ShouldFail()
        {
            // Arrange

            var itemsToAdd = MockData.TelescopeMockDataGenerator.CreateDataSet(111, partitionKey: "strictdeletefail");
            for (var i = 0; i < itemsToAdd.Length; i++)
            {
                itemsToAdd[i].MainImage = new LargeBlob(itemsToAdd[i].ProductId + ".blob", "just some data", Encoding.UTF8, "text/plain");
            }

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");
            store.UseClientSideValidation = true;

            // Act

            await store.InsertAsync(BatchingMode.Loose, itemsToAdd);


            // Assert

            var blobNames = itemsToAdd.Select(x => store.BuildBlobPath(_fixture.TableAndContainerNames["base"],
                x.CategoryId, x.ProductId, "MainImage", x.MainImage.Filename));

            var tasks = blobNames.Select(n => _fixture.AssertBlobExistsAsync("base", n));
            await Task.WhenAll(tasks);

            tasks = itemsToAdd.Select(item => _fixture.AssertTableEntityExistsAsync("base", item.CategoryId, item.ProductId));
            await Task.WhenAll(tasks);

            var itemsToDelete = itemsToAdd.Select(x => (x.CategoryId, x.ProductId)).ToArray();

            var exception = await Assert.ThrowsAsync<AzureTableDataStoreBatchedOperationException<TelescopePackageProduct>>(
                () => store.DeleteAsync(BatchingMode.Strong, itemsToDelete));

            exception.BatchExceptionContexts.Count.Should().Be(1);
            exception.BatchExceptionContexts[0].BatchEntities.Count.Should().Be(111);
            exception.InnerException.Should().BeOfType<AzureTableDataStoreInternalException>();

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T18_InsertBatches_StrictMode_ThenDeleteInStrictMode()
        {
            // Arrange

            var itemsToAdd = Enumerable.Range(0, 5).ToList().Select(x => new VerySimpleObject()
            {
                Id = "item" + x,
                Partition = "strictinsertdelete",
                Value = x
            });
            
            
            var store = _fixture.GetNewTableDataStore<VerySimpleObject>("simples");
            store.UseClientSideValidation = true;

            // Act

            await store.InsertAsync(BatchingMode.Strict, itemsToAdd.ToArray());


            // Assert

            var tasks = itemsToAdd.Select(item => _fixture.AssertTableEntityExistsAsync("simples", item.Partition, item.Id));
            await Task.WhenAll(tasks);

            var itemsToDelete = itemsToAdd.Select(x => (x.Partition, x.Id)).ToArray();

            await store.DeleteAsync(BatchingMode.Strict, itemsToDelete);

            tasks = itemsToAdd.Select(item => _fixture.AssertTableEntityDoesNotExistAsync("simples", item.Partition, item.Id));
            await Task.WhenAll(tasks);

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T19_InsertBatches_StrongMode_ThenEnumerateWithMultipleCalls()
        {
            var itemsToAdd = Enumerable.Range(0, 1600).ToList().Select(x => new VerySimpleObject()
            {
                Id = "item" + x,
                Partition = "enumeration1",
                Value = x
            });

            var store = _fixture.GetNewTableDataStore<VerySimpleObject>("simples");

            await store.InsertAsync(BatchingMode.Strong, itemsToAdd.ToArray());

            List<VerySimpleObject> entities = new List<VerySimpleObject>();
            TableContinuationToken continuation = null;

            await store.EnumerateWithMetadataAsync(x => x.Partition == "enumeration1", 500, async (ents, token) =>
            {
                entities.AddRange(ents.Select(x => x.Value));
                continuation = token;
                return false;
            });

            // Continue where we left off.

            await store.EnumerateWithMetadataAsync(x => x.Partition == "enumeration1", 500, async (ents, token) =>
            {
                entities.AddRange(ents.Select(x => x.Value));
                return true;
            }, continuation);

            entities.Count.Should().Be(1600);

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T20_InsertBatches_StrongMode_ThenCount()
        {
            var itemsToAdd = Enumerable.Range(0, 1100).ToList().Select(x => new VerySimpleObject()
            {
                Id = "item" + x,
                Partition = "count",
                Value = x
            });
            var itemsToAdd2 = Enumerable.Range(0, 10).ToList().Select(x => new VerySimpleObject()
            {
                Id = "item" + x,
                Partition = "other",
                Value = x
            });

            var store = _fixture.GetNewTableDataStore<VerySimpleObject>("simplescount");

            await store.InsertAsync(BatchingMode.Strong, itemsToAdd.Concat(itemsToAdd2).ToArray());


            var rowCount = await store.CountRowsAsync(x => x.Partition == "count");

            rowCount.Should().Be(1100L);

            rowCount = await store.CountRowsAsync();

            rowCount.Should().Be(1110L);
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T21_InsertBatches_StrongMode_ThenList()
        {
            var itemsToAdd = Enumerable.Range(0, 10).ToList().Select(x => new VerySimpleObject()
            {
                Id = "item" + x,
                Partition = "list1",
                Value = x
            });
            var itemsToAdd2 = Enumerable.Range(0, 10).ToList().Select(x => new VerySimpleObject()
            {
                Id = "item" + x,
                Partition = "list2",
                Value = x
            });

            var store = _fixture.GetNewTableDataStore<VerySimpleObject>("listing");

            await store.InsertAsync(BatchingMode.Strong, itemsToAdd.Concat(itemsToAdd2).ToArray());

            var itemList = await store.ListWithMetadataAsync();
            itemList.Count.Should().Be(20);

            var itemList2 = await store.ListAsync(null, 5);
            itemList2.Count.Should().Be(5);

        }


        //[Fact(/*Skip = "reason"*/)]
        //public async Task T16_InsertOrReplaceBatches_WithStrongMode_ValidationShouldThrow()
        //{
        //    // Arrange

        //    var itemsToInsertOrReplace = MockData.TelescopeMockDataGenerator.CreateDataSet(2, partitionKey: "strongvalidation1");

        //    for (var i = 0; i < itemsToInsertOrReplace.Length; i++)
        //    {
        //        itemsToInsertOrReplace[i].InternalReferenceId = Guid.Empty;
        //        itemsToInsertOrReplace[i].MainImage = new LargeBlob(itemsToInsertOrReplace[i].ProductId + ".blob", "xxx", Encoding.UTF8, "text/plain");
        //    }

        //    var store = GetTelescopeStore();
        //    store.UseClientSideValidation = true;

        //    // Act

        //    var exception = await Assert.ThrowsAsync<AzureTableDataStoreEntityValidationException<TelescopePackageProduct>>(
        //        () => store.InsertOrReplaceAsync(BatchingMode.Strong, itemsToInsertOrReplace));

        //    exception.EntityValidationErrors.Count.Should().Be(10);

        //}
    }
}