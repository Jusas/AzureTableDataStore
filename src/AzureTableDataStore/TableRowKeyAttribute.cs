using System;

namespace AzureTableDataStore
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class TableRowKeyAttribute : Attribute
    {
        public TableRowKeyAttribute()
        {

        }
    }
}