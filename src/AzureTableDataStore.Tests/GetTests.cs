using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using AzureTableDataStore.Tests.Models;
using Xunit;

namespace AzureTableDataStore.Tests
{
    public class GetTests
    {
        [Fact]
        public async Task Should_get()
        {
            var store = new TableDataStore<UserProfile>("UseDevelopmentStorage=true", "userprofiles",
                "userprofilesblobs", PublicAccessType.None, "UseDevelopmentStorage=true");

            await store.GetAsync(x => !x.ExtendedProperties.HasVisitedBefore);
        }
    }
}