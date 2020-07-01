using System.Collections.Generic;
using FluentAssertions;
using Microsoft.Azure.Cosmos.Table;
using Xunit;

namespace AzureTableDataStore.Tests.InternalTests
{
    
    public class ReflectionUtilsTests
    {

        public class TableRow
        {
            public string Name { get; set; }
            public StoredBlob Document { get; set; }
            public Attachment RelatedItem { get; set; }
            public Dictionary<string, int> Dict { get; set; }
        }

        public class Attachment
        {
            public string Name { get; set; }
            public StoredBlob File { get; set; }
            public Attachment NestedAttachment { get; set; }
            public List<string> Keywords { get; set; }
        }


        [Fact]
        public void Should_gather_BlobRefs_recursively_correctly()
        {

            var testObject = new TableRow()
            {
                Name = "Outer",
                Document = StoredBlob.FromString("", "document"),
                RelatedItem = new Attachment()
                {
                    Name = "Att-01",
                    File = StoredBlob.FromString("", "att01"),
                    NestedAttachment = new Attachment()
                    {
                        Name = "Att01-Inner",
                        File = StoredBlob.FromString("", "att01-inner"),
                        NestedAttachment = null
                    }
                },
                Dict = null
            };

            var collected = ReflectionUtils.GatherPropertiesWithBlobsRecursive(testObject, new EntityPropertyConverterOptions());

            collected.Count.Should().Be(3);

        }


        [Fact]
        public void Should_gather_Blob_and_CollectionRefs_recursively_correctly_and_EntityPropertyConverter_should_successfully_flatten()
        {

            var testObject = new TableRow()
            {
                Name = "Outer",
                Document = StoredBlob.FromString("", "document"),
                RelatedItem = new Attachment()
                {
                    Name = "Att-01",
                    File = StoredBlob.FromString("", "att01"),
                    NestedAttachment = new Attachment()
                    {
                        Name = "Att01-Inner",
                        File = StoredBlob.FromString("", "att01-inner"),
                        NestedAttachment = null,
                        Keywords = new List<string>() { "alpha", "bravo" }
                    }
                },
                Dict = new Dictionary<string, int>()
                {
                    {"Key", 123 }
                }
            };

            var collectedBlobPropertyRefs = ReflectionUtils.GatherPropertiesWithBlobsRecursive(testObject, new EntityPropertyConverterOptions());
            var collectedCollectionPropertyRefs = ReflectionUtils.GatherPropertiesWithCollectionsRecursive(testObject, new EntityPropertyConverterOptions());

            foreach (var blobPropRef in collectedBlobPropertyRefs)
                blobPropRef.Property.SetValue(blobPropRef.SourceObject, null);

            foreach (var collectionPropertyRef in collectedCollectionPropertyRefs)
                collectionPropertyRef.Property.SetValue(collectionPropertyRef.SourceObject, null);

            var flattened = EntityPropertyConverter.Flatten(testObject, new EntityPropertyConverterOptions(), null);
            
            collectedBlobPropertyRefs.Count.Should().Be(3);
            collectedCollectionPropertyRefs.Count.Should().Be(2);

        }
    }
}