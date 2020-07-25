namespace AzureTableDataStore
{
    /// <summary>
    /// Utilities for Table Entity strings.
    /// </summary>
    public static class TableString
    {
        /// <summary>
        /// Notation to allow lt, lte, gt and gte comparison of strings in query expressions. <br/>
        /// Allows you to say e.g.: <br/>
        /// <c>entity => entity.RowKey.AsComparable() &gt; "Charlie".AsComparable()</c>
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        public static int AsComparable(this string source) => 0;
    }
}