using System;
using System.Collections.Generic;

namespace AzureTableDataStore.Tests.Models
{
    public class TestClass1
    {
        [TableRowKey]
        public string MyEntityId { get; set; }
        [TablePartitionKey]
        public string MyEntityPartitionKey { get; set; }

        public string MyStringValue { get; set; }
        public List<string> MyListOfStrings { get; set; }
        public DateTime MyDate { get; set; }
        public Guid MyGuid { get; set; }
        public long MyLong { get; set; }
        public int MyInt { get; set; }
        public EntitySubClass MySubClass { get; set; }
        public Dictionary<string, EntitySubClass> MyDictionaryOfSubClasses { get; set; }
        public LargeBlob MyLargeBlob { get; set; }
    }

    public class EntitySubClass
    {
        public string MyValue { get; set; }
    }
}