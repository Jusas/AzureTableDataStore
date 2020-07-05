using System.Collections.Generic;

namespace AzureTableDataStore.Tests.Models
{
    public class UserProfile
    {
        public class ProfileProperties
        {
            public bool HasVisitedBefore { get; set; }
            public long AverageVisitLengthSeconds { get; set; }
        }

        public class ProfileImages
        {
            public LargeBlob Current { get; set; }
            public LargeBlob Old { get; set; }
        }

        [TableRowKey]
        public string UserId { get; set; }

        [TablePartitionKey]
        public string UserType { get; set; }

        public string Name { get; set; }
        public int Age { get; set; }
        public List<string> Aliases { get; set; } = new List<string>();
        public ProfileProperties ExtendedProperties { get; set; }

        public ProfileImages ProfileImagery { get; set; }
    }
}