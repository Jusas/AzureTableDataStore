using System;

namespace AzureTableDataStore
{
    /// <summary>
    /// An attribute that marks a property to be the Table Row Key.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class TableRowKeyAttribute : Attribute
    {
    }
}