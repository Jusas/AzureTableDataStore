using System;

namespace AzureTableDataStore
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class TablePartitionKeyAttribute : Attribute
    {
        public TablePartitionKeyAttribute()
        {
            
        }
    }
}