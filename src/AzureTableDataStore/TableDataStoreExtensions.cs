using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace AzureTableDataStore
{
    public static class TableDataStoreExtensions
    {
        public static ITableDataStore NamedInstance(this IEnumerable<ITableDataStore> stores, string name) 
            => stores.FirstOrDefault(x => x.Name == name);

        public static ITableDataStore<TData> NamedInstance<TData>(this IEnumerable<ITableDataStore<TData>> stores, string name) where TData:new()
            => stores.FirstOrDefault(x => x.Name == name);
    }
}