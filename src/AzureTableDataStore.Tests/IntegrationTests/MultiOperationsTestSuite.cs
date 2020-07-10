using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using AzureTableDataStore.Tests.Infrastructure;
using AzureTableDataStore.Tests.Models;
using FluentAssertions;
using Xunit;

namespace AzureTableDataStore.Tests.IntegrationTests
{
    [TestCaseOrderer("AzureTableDataStore.Tests.Infrastructure.AlphabeticalTestCaseOrderer", "AzureTableDataStore.Tests")]
    public class MultiOperationsTestSuite : IClassFixture<StorageContextFixture>
    {

        private StorageContextFixture _storageContextFixture;
        private BlobStorageAssertions _blobStorageAssertions;

        public MultiOperationsTestSuite(StorageContextFixture fixture)
        {
            _storageContextFixture = fixture;
            _blobStorageAssertions = new BlobStorageAssertions(fixture.ConnectionString, fixture.TableAndContainerName);
        }

        public TableDataStore<TelescopePackageProduct> GetTelescopeStore()
        {
            return new TableDataStore<TelescopePackageProduct>(_storageContextFixture.ConnectionString, _storageContextFixture.TableAndContainerName,
                _storageContextFixture.TableAndContainerName, PublicAccessType.None, _storageContextFixture.ConnectionString);
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T01_InsertMultiple_NonBatched_WithBlobs()
        {
            // Arrange

            var testDataSet = MockData.TelescopeMockDataGenerator.SmallDataSet;
            var store = GetTelescopeStore();


            // Act

            await store.InsertAsync(false, testDataSet.ToArray());

            // Assert

            // The following test should assert.
        }


        [Fact(/*Skip = "reason"*/)]
        public async Task T02_FindMultiple_ShouldReturnFullObjects_WithBlobs()
        {
            // Arrange

            var store = GetTelescopeStore();


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
            var store = GetTelescopeStore();

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

            await store.MergeAsync(false, x => new
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

            var store = GetTelescopeStore();
            var testDataSet = await store.FindAsync((x, dt) => dt < DateTime.UtcNow);

            testDataSet.Count.Should().Be(4);

            // Images for Telescope 1 and Telescope 2 should get deleted, while new ones should be added for 3 and 4.

            var newBlobFor3 = new LargeBlob("newimage.png", () => new FileStream("Resources/meade-telescope-n-2001000-lx85-goto.png", FileMode.Open, FileAccess.Read));
            var newBlobFor4 = new LargeBlob("newimage.png", () => new FileStream("Resources/meade-telescope-n-2001000-lx85-goto.png", FileMode.Open, FileAccess.Read));

            var mainImageFilename1 = testDataSet[0].MainImage.Filename;
            var mainImageFilename2 = testDataSet[1].MainImage.Filename;
            var mainImageFilename3 = newBlobFor3.Filename;
            var mainImageFilename4 = newBlobFor4.Filename;

            var blobPath1 = store.BuildBlobPath(_storageContextFixture.TableAndContainerName, testDataSet[0].CategoryId,
                testDataSet[0].ProductId, "MainImage", mainImageFilename1);
            var blobPath2 = store.BuildBlobPath(_storageContextFixture.TableAndContainerName, testDataSet[1].CategoryId,
                testDataSet[1].ProductId, "MainImage", mainImageFilename2);
            var blobPath3 = store.BuildBlobPath(_storageContextFixture.TableAndContainerName, testDataSet[2].CategoryId,
                testDataSet[2].ProductId, "MainImage", mainImageFilename3);
            var blobPath4 = store.BuildBlobPath(_storageContextFixture.TableAndContainerName, testDataSet[3].CategoryId,
                testDataSet[3].ProductId, "MainImage", mainImageFilename4);

            _blobStorageAssertions.BlobExists(blobPath1);
            _blobStorageAssertions.BlobExists(blobPath2);
            _blobStorageAssertions.BlobDoesNotExist(blobPath3);
            _blobStorageAssertions.BlobDoesNotExist(blobPath4);

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

            await store.MergeAsync(false, x => new
            {
                x.MainImage,
                x.Name,
                x.Description
            }, LargeBlobNullBehavior.DeleteBlob, testDataSet.ToArray());


            // Assert

            _blobStorageAssertions.BlobExists(blobPath3);
            _blobStorageAssertions.BlobExists(blobPath4);
            _blobStorageAssertions.BlobDoesNotExist(blobPath1);
            _blobStorageAssertions.BlobDoesNotExist(blobPath2);

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T05_ReplaceMultiple_NonBatched_ShouldDeleteBlobsWhenPropertiesSetToNull()
        {
            // Relies on T01 insert.

            // Arrange

            var store = GetTelescopeStore();

            var allItems = await store.FindAsync((x, dt) => dt < DateTimeOffset.UtcNow);

            allItems.Count.Should().Be(4);

            // We're gonna set the MainImages to null, and this should cause the deletion of the blobs already uploaded to storage.

            var blobPath3 = store.BuildBlobPath(_storageContextFixture.TableAndContainerName, allItems[2].CategoryId,
                allItems[2].ProductId, "MainImage", allItems[2].MainImage.Filename);
            var blobPath4 = store.BuildBlobPath(_storageContextFixture.TableAndContainerName, allItems[3].CategoryId,
                allItems[3].ProductId, "MainImage", allItems[3].MainImage.Filename);

            _blobStorageAssertions.BlobExists(blobPath3);
            _blobStorageAssertions.BlobExists(blobPath4);

            allItems[2].MainImage = null;
            allItems[2].Name = "Replaced 3";
            allItems[3].MainImage = null;
            allItems[3].Name = "Replaced 4";

            await store.InsertOrReplaceAsync(false, allItems.Skip(2).Take(2).ToArray());

            _blobStorageAssertions.BlobDoesNotExist(blobPath3);
            _blobStorageAssertions.BlobDoesNotExist(blobPath4);

        }


        [Fact(/*Skip = "reason"*/)]
        public async Task T06_MergeMultiple_Batched_UsingEtags_SelectedProperties()
        {
            // Relies on T01 insert.

            // Arrange
            
            var store = GetTelescopeStore();


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

            // Here the LargeBlobNullBehavior.IgnoreProperty is meaningless, since the property is not selected for merging.

            await store.MergeAsync(true, x => new
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

            var store = GetTelescopeStore();

            // All items that come alphabetically before "Omegon"
            var allBeforeO = await store.FindAsync(x => x.Name.AsComparable() < "O".AsComparable());

            Assert.True(allBeforeO.All(x => !x.Name.StartsWith("O")));
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T08_FindWithMetadata()
        {
            // Arrange

            var store = GetTelescopeStore();

            // All items that are for astrophotography
            var allForAstrophotography = await store.FindWithMetadataAsync(x => x.Specifications.ForAstrophotography == true);

            Assert.True(allForAstrophotography.All(x => x.Value.Specifications.ForAstrophotography == true));
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T09_FindWithMetadata_WithDate()
        {
            // Arrange

            var store = GetTelescopeStore();

            // All items that are for astrophotography and older than now
            var allForAstrophotography = await store.FindWithMetadataAsync((x, dt) => x.Specifications.ForAstrophotography == true && dt < DateTime.UtcNow);

            Assert.True(allForAstrophotography.All(x => x.Value.Specifications.ForAstrophotography == true));
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T10_InsertHugeBatch_WithoutBlobs()
        {
            // Arrange

            // Set images to null so that we can use the batch insert with this data model.
            var itemsToAdd = MockData.TelescopeMockDataGenerator.CreateDataSet(10_000);
            for (var i = 0; i < itemsToAdd.Length; i++)
            {
                itemsToAdd[i].MainImage = null;
            }

            var store = GetTelescopeStore();

            // Act

            // 7500 to partition 1, and 2500 to partition 2, with 100 per batch. 100 batches.
            await store.InsertAsync(true, itemsToAdd);

            // Should not throw.
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T11_InsertBatches_WithVeryLargeContent_WithoutBlobs()
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

            var store = GetTelescopeStore();

            // Act

            await store.InsertAsync(true, itemsToAdd);

            // Should not throw, should succeed by splitting the content into multiple batches.
        }

        [Fact(/*Skip = "reason"*/)]
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

            var store = GetTelescopeStore();
            store.UseClientSideValidation = true;

            // Act

            // Client side validation should catch this early.

            var exception = await Assert.ThrowsAsync<AzureTableDataStoreEntityValidationException>(() => store.InsertAsync(true, itemsToAdd));

            exception.EntityValidationErrors.Count.Should().Be(3);

        }

    }
}