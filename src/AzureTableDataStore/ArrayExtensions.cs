using System;
using System.Collections.Generic;
using System.Linq;

namespace AzureTableDataStore
{
    internal static class ArrayExtensions
    {
        /// <summary>
        /// Splits a collection to smaller batches.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="list"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
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