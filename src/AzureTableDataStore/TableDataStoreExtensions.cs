using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace AzureTableDataStore
{
    /// <summary>
    /// Extensions to get named TableDataStore instances from an IEnumerable.
    /// Convenient when needing to inject multiple instances via DI.
    /// </summary>
    public static class TableDataStoreExtensions
    {
        public static INamedTableDataStore NamedInstance(this IEnumerable<INamedTableDataStore> stores, string name) 
            => stores.FirstOrDefault(x => x.Name == name);

        public static ITableDataStore<TData> NamedInstance<TData>(this IEnumerable<ITableDataStore<TData>> stores, string name) where TData:new()
            => stores.FirstOrDefault(x => x.Name == name);
    }
}