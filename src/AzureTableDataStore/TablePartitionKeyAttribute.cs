using System;

namespace AzureTableDataStore
{
    /// <summary>
    /// An attribute that marks a property to be the Table Partition Key.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class TablePartitionKeyAttribute : Attribute
    {
    }
}