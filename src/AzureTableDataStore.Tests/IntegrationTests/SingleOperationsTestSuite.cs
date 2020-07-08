using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using AzureTableDataStore.Tests.Models;
using FluentAssertions;
using Xunit;

namespace AzureTableDataStore.Tests.IntegrationTests
{
    [TestCaseOrderer("AzureTableDataStore.Tests.Infrastructure.AlphabeticalTestCaseOrderer", "AzureTableDataStore.Tests")]
    public class SingleOperationsTestSuite : IClassFixture<StorageContextFixture>
    {

        public TableDataStore<TelescopePackageProduct> GetTelescopeStore()
        {
            return new TableDataStore<TelescopePackageProduct>(_storageContextFixture.ConnectionString, _storageContextFixture.TableName,
                _storageContextFixture.ContainerName, PublicAccessType.None, _storageContextFixture.ConnectionString);
        }

        private StorageContextFixture _storageContextFixture;
        
        public SingleOperationsTestSuite(StorageContextFixture fixture)
        {
            _storageContextFixture = fixture;
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T01_Insert_One_WithLargeBlob()
        {
            // Arrange

            var newItem = MockData.TelescopeMockDataGenerator.CreateDataSet(1).First();

            // Act

            var store = GetTelescopeStore();
            await store.InsertAsync(false, newItem);

            // No assertions (assume ok if no exceptions thrown)
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T02_Get_One_WithLargeBlob()
        {
            // Arrange

            var store = GetTelescopeStore();

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
            var store = GetTelescopeStore();

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
            var store = GetTelescopeStore();

            var result = await store.GetAsync((item, ts) =>
                item.CategoryId == "telescopes-full" && item.ProductId == "omegon-ac-70-700-az20" && ts < DateTime.UtcNow);

            Assert.NotNull(result);
        }

        [Fact(/*Skip = "reason"*/)]
        public async Task T05_Get_One_WithMetadata()
        {
            var store = GetTelescopeStore();

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
            var store = GetTelescopeStore();

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
            var store = GetTelescopeStore();

            // Same row and partition key as the previously created instance.
            var item = MockData.TelescopeMockDataGenerator.CreateDataSet(1).First();

            item.Name = "replaced";
            item.Specifications.Mount.Type = "replaced";
            
            await store.InsertOrReplaceAsync(false, item);

            var fromTable = await store.GetAsync(x => x.ProductId == item.ProductId);

            fromTable.Name.Should().Be("replaced");
            fromTable.Specifications.Mount.Type.Should().Be("replaced");


        }


    }
}
