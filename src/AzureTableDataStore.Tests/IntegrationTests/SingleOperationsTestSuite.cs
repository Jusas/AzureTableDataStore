using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using AzureTableDataStore.Tests.Infrastructure;
using AzureTableDataStore.Tests.Models;
using FluentAssertions;
using Xunit;

namespace AzureTableDataStore.Tests.IntegrationTests
{
    [TestCaseOrderer("AzureTableDataStore.Tests.Infrastructure.AlphabeticalTestCaseOrderer", "AzureTableDataStore.Tests")]
    public class SingleOperationsTestSuite : IClassFixture<StorageContextFixture>
    {
        private StorageContextFixture _fixture;
        
        public SingleOperationsTestSuite(StorageContextFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T01a_Insert_One_WithLargeBlob_ctor1()
        {
            // Arrange

            var testContext = "largeblobtests";

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1, "blobctor1").First();
            newItem.MainImage = new LargeBlob("largeblob.txt",
                () => new MemoryStream(Encoding.UTF8.GetBytes("testing")), "text/plain");
            
            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);
            await store.InsertAsync(BatchingMode.None, newItem);

            
            _fixture.AssertTableEntityExists(testContext, newItem.CategoryId, newItem.ProductId);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames[testContext],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);
            _fixture.AssertBlobExists(testContext, blobPath);
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T01b_Insert_One_WithLargeBlob_ctor2()
        {
            // Arrange

            var testContext = "largeblobtests";

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1, "blobctor2").First();
            newItem.MainImage = new LargeBlob("largeblob.txt", () => Task.FromResult((Stream)new MemoryStream(Encoding.UTF8.GetBytes("testing"))), "text/plain");

            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);
            await store.InsertAsync(BatchingMode.None, newItem);


            _fixture.AssertTableEntityExists(testContext, newItem.CategoryId, newItem.ProductId);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames[testContext],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);
            _fixture.AssertBlobExists(testContext, blobPath);
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T01c_Insert_One_WithLargeBlob_ctor3()
        {
            // Arrange

            var testContext = "largeblobtests";

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1, "blobctor3").First();
            newItem.MainImage = new LargeBlob("largeblob.txt", new MemoryStream(Encoding.UTF8.GetBytes("testing")),
                "text/plain");

            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);
            await store.InsertAsync(BatchingMode.None, newItem);


            _fixture.AssertTableEntityExists(testContext, newItem.CategoryId, newItem.ProductId);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames[testContext],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);
            _fixture.AssertBlobExists(testContext, blobPath);
        }


        [Fact(/*Skip = "reason"*/)]
        public async Task T01d_Insert_One_WithLargeBlob_ctor4()
        {
            // Arrange

            var testContext = "largeblobtests";

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1, "blobctor4").First();
            newItem.MainImage = new LargeBlob("largeblob.txt", Encoding.UTF8.GetBytes("testing"), "text/plain");

            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);
            await store.InsertAsync(BatchingMode.None, newItem);


            _fixture.AssertTableEntityExists(testContext, newItem.CategoryId, newItem.ProductId);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames[testContext],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);
            _fixture.AssertBlobExists(testContext, blobPath);
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T01e_Insert_One_WithLargeBlob_ctor5()
        {
            // Arrange

            var testContext = "largeblobtests";

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1, "blobctor5").First();
            newItem.MainImage = new LargeBlob("largeblob.txt", "testing", Encoding.Unicode, "text/plain");

            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);
            await store.InsertAsync(BatchingMode.None, newItem);


            _fixture.AssertTableEntityExists(testContext, newItem.CategoryId, newItem.ProductId);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames[testContext],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);
            _fixture.AssertBlobExists(testContext, blobPath);
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T02a_Insert_One_WithLargeBlob()
        {
            // Arrange

            var testContext = "base";

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1).First();

            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext);
            await store.InsertAsync(BatchingMode.None, newItem);

            _fixture.AssertTableEntityExists(testContext, newItem.CategoryId, newItem.ProductId);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames[testContext],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);
            _fixture.AssertBlobExists(testContext, blobPath);
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T02b_Get_One_WithLargeBlob()
        {
            // Arrange

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            // Act

            var result = await store.GetAsync(x => x.CategoryId == "telescopes-full" && x.ProductId == "omegon-ac-70-700-az20");

            // Assert

            Assert.NotNull(result);

            result.Name.Should().Be("Omegon Telescope AC 70/700 AZ-2 (0)");
            result.CategoryId.Should().Be("telescopes-full");
            result.ProductId.Should().Be("omegon-ac-70-700-az20");
            result.Description.Should().Be(
                "The Omegon AC 70/700 telescope is your first taste of the world of astronomy. Practical observing with it is so simple that it highly suitable for children and adults alike.\r\n\r\n\r\nThe instrument is simple to understand and is very quick to set up, without any tools being required. Simply set it up, insert eyepiece and observe!");
            result.PackageDepthMm.Should().Be(2200);
            result.PackageHeightMm.Should().Be(500);
            result.PackageWidthMm.Should().Be(500);
            result.AddedToInventory.Should().Be(new DateTime(2017, 1, 2, 0, 0, 0, DateTimeKind.Utc));
            result.InternalReferenceId.Should().Be(new Guid(1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4));
            result.SoldItems.Should().Be(10);
            result.SearchNames.Should()
                .ContainInOrder("Omegon Telescope AC 70/700 AZ-2", "Omegon AC 70/700", "Omegon AZ-2");
            result.Specifications.ApplicationDescription.Should().Be("General visual observation of sky and nature");
            result.Specifications.ForAstrophotography.Should().Be(false);
            result.Specifications.ForVisualObservation.Should().Be(true);
            result.Specifications.Mount.Type.Should().Be("AZ-2");
            result.Specifications.Mount.GotoControl.Should().Be(false);
            result.Specifications.Mount.Mounting.Should().Be(MountingType.Azimuthal);
            result.Specifications.Mount.Tracking.Should().Be(false);
            result.Specifications.Optics.Type.Should().Be("Refractor");
            result.Specifications.Optics.ApertureMm.Should().Be(70);
            result.Specifications.Optics.ApertureRatioF.Should().Be(10);
            result.Specifications.Optics.FocalLengthMm.Should().Be(700);
            result.Specifications.Tripod.HeightDescription.Should().Be("66-120mm adjustable");
            result.Specifications.Tripod.Material.Should().Be("Aluminum");
            result.Specifications.Tripod.WeightKg.Should().Be(2);

            result.MainImage.Length.Should().Be(22_625L);
            result.MainImage.ContentType.Should().Be("application/octet-stream"); // this will change
            result.MainImage.Filename.Should().Be("omegon-ac-70700-az2.png");
            using (var imageStream = await result.MainImage.AsyncDataStream.Value)
            {
                using (var reader = new BinaryReader(imageStream))
                {
                    var dataBytes = reader.ReadBytes((int)result.MainImage.Length);
                    dataBytes.Length.Should().Be((int)result.MainImage.Length);
                }
            }

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T03_Get_One_WithSelectedProperties()
        {
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            var result = await store.GetAsync(x => x.CategoryId == "telescopes-full" && x.ProductId == "omegon-ac-70-700-az20", telescope => new
            {
                telescope.Name,
                telescope.PackageDepthMm,
                telescope.CategoryId,
                telescope.Specifications.Mount.Type
            });

            result.Name.Should().Be("Omegon Telescope AC 70/700 AZ-2 (0)");
            result.PackageDepthMm.Should().Be(2200);
            result.CategoryId.Should().Be("telescopes-full");
            result.Description.Should().BeNull();
            result.Specifications.Mount.Type.Should().Be("AZ-2");

            result.ProductId.Should().Be("omegon-ac-70-700-az20"); // row key is always populated, for obvious reasons (as well as partition key).
            result.PackageHeightMm.Should().Be(default);
            result.PackageWidthMm.Should().Be(default);
            result.SearchNames.Should().BeNull();
            result.Specifications.Optics.Should().BeNull();
            result.Specifications.Tripod.Should().BeNull();
            result.MainImage.Should().BeNull();
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T04_Get_One_WithTimestampClause()
        {
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            var result = await store.GetAsync((item, ts) =>
                item.CategoryId == "telescopes-full" && item.ProductId == "omegon-ac-70-700-az20" && ts < DateTime.UtcNow);

            Assert.NotNull(result);
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T05_Get_One_WithMetadata()
        {
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            var result = await store.GetWithMetadataAsync(item =>
                item.CategoryId == "telescopes-full" && item.ProductId == "omegon-ac-70-700-az20");

            Assert.NotNull(result);

            result.ETag.Should().NotBeNullOrEmpty();
            result.Timestamp.Should().NotBeNull();
            result.Value.Should().NotBeNull();

            result.Value.ProductId.Should().Be("omegon-ac-70-700-az20");
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T06_Get_One_WithMetadataAndTimestampClause()
        {
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            var result = await store.GetWithMetadataAsync((item, ts) =>
                item.CategoryId == "telescopes-full" && item.ProductId == "omegon-ac-70-700-az20" && ts < DateTime.UtcNow);

            Assert.NotNull(result);

            result.ETag.Should().NotBeNullOrEmpty();
            result.Timestamp.Should().NotBeNull();
            result.Value.Should().NotBeNull();

            result.Value.ProductId.Should().Be("omegon-ac-70-700-az20");
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T07_InsertOrReplace_One()
        {
            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("base");

            // Same row and partition key as the previously created instance.
            var item = MockData.TelescopeMockDataGenerator.CreateDataSet(1).First();

            item.Name = "replaced";
            item.Specifications.Mount.Type = "replaced";
            
            await store.InsertOrReplaceAsync(BatchingMode.None, item);

            var fromTable = await store.GetAsync(x => x.ProductId == item.ProductId);

            fromTable.Name.Should().Be("replaced");
            fromTable.Specifications.Mount.Type.Should().Be("replaced");


        }


        [Fact(/*Skip = "reason"*/)]
        public async Task T08_Insert_One_WithLargeBlob_Then_DeleteOne()
        {
            // Arrange

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1).First();
            newItem.ProductId = "tobedeleted";

            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("t08");
            await store.InsertAsync(BatchingMode.None, newItem);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames["t08"],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);

            _fixture.AssertTableEntityExists("t08", newItem.CategoryId, newItem.ProductId);
            _fixture.AssertBlobExists("t08", blobPath);
            
            await store.DeleteAsync(BatchingMode.None, newItem);

            _fixture.AssertTableEntityDoesNotExist("t08", newItem.CategoryId, newItem.ProductId);
            _fixture.AssertBlobDoesNotExist("t08", blobPath);

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T09_Insert_One_WithLargeBlob_Then_DeleteOne_UsingJustEntityKeys()
        {
            // Arrange

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1).First();
            newItem.ProductId = "tobedeleted2";

            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>("t09");
            await store.InsertAsync(BatchingMode.None, newItem);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames["t09"],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);

            _fixture.AssertTableEntityExists("t09", newItem.CategoryId, newItem.ProductId);
            _fixture.AssertBlobExists("t09", blobPath);

            await store.DeleteAsync(BatchingMode.None, (newItem.CategoryId, newItem.ProductId));

            _fixture.AssertTableEntityDoesNotExist("t09", newItem.CategoryId, newItem.ProductId);
            _fixture.AssertBlobDoesNotExist("t09", blobPath);

        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T10_Insert_One_WithBlob_ThenGetBlobUrlWithSas()
        {
            // Arrange

            var testContext = "t10";

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1).First();
            newItem.ProductId = "forgettinguri";

            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext, isPublic: false);
            await store.InsertAsync(BatchingMode.None, newItem);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames[testContext],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);

            _fixture.AssertTableEntityExists(testContext, newItem.CategoryId, newItem.ProductId);
            _fixture.AssertBlobExists(testContext, blobPath);

            var url = newItem.MainImage.GetDownloadUrl(true, TimeSpan.FromMinutes(10));
            var urlPublic = newItem.MainImage.GetDownloadUrl();

            // Should not throw.

            using (var httpClient = new HttpClient())
            {
                var httpResponse = await httpClient.GetAsync(url);
                httpResponse.IsSuccessStatusCode.Should().Be(true);
                var bytes = await httpResponse.Content.ReadAsByteArrayAsync();
                bytes.Length.Should().Be((int) newItem.MainImage.Length);
            }
            
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T11_Insert_One_WithBlob_ThenGetBlobUrlNoSas()
        {
            // Arrange

            var testContext = "t11";

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1).First();
            newItem.ProductId = "forgettinguri";

            // Act

            var store = _fixture.GetNewTableDataStore<TelescopePackageProduct>(testContext, isPublic: true);
            await store.InsertAsync(BatchingMode.None, newItem);

            var blobPath = store.BuildBlobPath(_fixture.TableAndContainerNames[testContext],
                newItem.CategoryId, newItem.ProductId, "MainImage", newItem.MainImage.Filename);

            _fixture.AssertTableEntityExists(testContext, newItem.CategoryId, newItem.ProductId);
            _fixture.AssertBlobExists(testContext, blobPath);

            var url = newItem.MainImage.GetDownloadUrl();

            // Should not throw.

            using (var httpClient = new HttpClient())
            {
                var httpResponse = await httpClient.GetAsync(url);
                httpResponse.IsSuccessStatusCode.Should().Be(true);
                var bytes = await httpResponse.Content.ReadAsByteArrayAsync();
                bytes.Length.Should().Be((int)newItem.MainImage.Length);
            }

        }

    }
}
