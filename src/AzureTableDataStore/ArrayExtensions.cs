using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace AzureTableDataStore
{
    public static class ArrayExtensions
    {

        public static IEnumerable<IEnumerable<T>> SplitToBatches<T>(this IEnumerable<T> list, int batchSize)
        {
            var batches = Math.Ceiling((float) list.Count() / batchSize);

            for (var i = 0; i < batches; i++)
            {
                yield return list.Skip(i * batchSize).Take(batchSize);
            }
        }

    }
}