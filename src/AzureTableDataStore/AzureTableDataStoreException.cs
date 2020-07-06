using System;
using System.Collections.Generic;

namespace AzureTableDataStore
{
    public class AzureTableDataStoreException : Exception
    {
        public enum ProblemSourceType
        {
            General,
            TableStorage,
            BlobStorage,
            Configuration,
            Data
        }

        public ProblemSourceType ProblemSource { get; } = ProblemSourceType.General;
        public IDictionary<object, Exception> EntityErrors { get; internal set; }

        public AzureTableDataStoreException(string message, ProblemSourceType problemSource, Exception inner = null) : base(message, inner)
        {
            ProblemSource = problemSource;
        }

        public AzureTableDataStoreException(string message, Exception inner = null) : base(message, inner) 
        {
        }
    }
}