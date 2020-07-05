using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using AzureTableDataStore.Tests.Models;
using FluentAssertions;
using Xunit;

namespace AzureTableDataStore.Tests
{
    public class GetTests
    {
        [Fact]
        public async Task Should_insert_and_get()
        {
            var store = new TableDataStore<UserProfile>("UseDevelopmentStorage=true", "userprofiles",
                "userprofilesblobs", PublicAccessType.None, "UseDevelopmentStorage=true");

            var dataRow = new UserProfile()
            {
                Age = 55,
                Name = "James Bond",
                Aliases = new List<string>() { "Cmdr Bond", "James", "007" },
                ExtendedProperties = new UserProfile.ProfileProperties()
                {
                    AverageVisitLengthSeconds = 5,
                    HasVisitedBefore = true
                },
                UserType = "agent",
                UserId = "007",
                ProfileImagery = new UserProfile.ProfileImages()
                {
                    Current = new LargeBlob("bond_new.png", new FileStream("Resources/bond_new.png", FileMode.Open, FileAccess.Read)),
                    Old = new LargeBlob("bond_old.png", new FileStream("Resources/bond_old.png", FileMode.Open, FileAccess.Read)),
                }
            };

            //await store.InsertAsync(dataRow);

            var fetchedRow = await store.GetAsync(x => x.UserId == "007" && x.UserType == "agent");
            byte[] fetchedBytes;
           
            using (var imageDataStream = await fetchedRow.ProfileImagery.Current.AsyncDataStream.Value)
            {
                using (var reader = new BinaryReader(imageDataStream))
                {
                    // Note: imageDataStream.Length is not supported (HttpBaseStream does not support it)
                    fetchedBytes = reader.ReadBytes((int)fetchedRow.ProfileImagery.Current.Length);
                }
            }

            var inputBytes = File.ReadAllBytes("Resources/bond_new.png");

            fetchedBytes.Should().BeEquivalentTo(inputBytes);

        }
    }
}