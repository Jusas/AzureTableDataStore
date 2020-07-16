namespace AzureTableDataStore
{
    public static class TableString
    {
        /// <summary>
        /// Notation to allow lt, lte, gt and gte comparison of strings in query expressions.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public static int AsComparable(this string source) => 0;
    }
}