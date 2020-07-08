using System;
using System.Collections.Generic;

namespace AzureTableDataStore
{
    /// <summary>
    /// Indicates the problem source.
    /// </summary>
    public enum ProblemSourceType
    {
        /// <summary>
        /// A general (unspecified, unexpected) exception.
        /// </summary>
        General,

        /// <summary>
        /// An exception that is originating from an Azure Table Storage operation.
        /// </summary>
        TableStorage,

        /// <summary>
        /// An exception that is originating from an Azure Blob Storage operation.
        /// </summary>
        BlobStorage,

        /// <summary>
        /// A configuration issue.
        /// </summary>
        Configuration,

        /// <summary>
        /// A data issue.
        /// </summary>
        Data
    }


    /// <summary>
    /// Exception representing an error in configuration.
    /// </summary>
    public class AzureTableDataStoreConfigurationException : Exception
    {
        public AzureTableDataStoreConfigurationException(string message, Exception inner = null) : base(message, inner)
        {
            
        }
    }

    /// <summary>
    /// Exception representing an internal failure inside TableDataStore.
    /// </summary>
    public class AzureTableDataStoreInternalException : Exception
    {
        /// <summary>
        /// The problem source, indicating what was the service or reason triggering this exception.
        /// </summary>
        public ProblemSourceType ProblemSource { get; set; }

        public AzureTableDataStoreInternalException(string message, ProblemSourceType problemSource, Exception inner = null) : base(message, inner)
        {
            ProblemSource = problemSource;
        }

        public AzureTableDataStoreInternalException(string message, Exception inner = null) : base(message, inner)
        {

        }
    }

    /// <summary>
    /// Exception representing an TableDataStore query exception.
    /// </summary>
    public class AzureTableDataStoreQueryException : Exception
    {
        public AzureTableDataStoreQueryException(string message, Exception inner = null) : base(message, inner)
        {
        }

    }

    /// <summary>
    /// Exception representing a failure inside an Azure Table Storage batch operation.
    /// </summary>
    public class AzureTableDataStoreBatchedOperationException : Exception
    {
        /// <summary>
        /// A collection of Key-Value pairs of the Exception and the entities contained in the failed batch.
        /// </summary>
        public IDictionary<Exception, object[]> FailedBatches { get; internal set; } = new Dictionary<Exception, object[]>();
        
        /// <summary>
        /// The problem source, indicating what was the service or reason triggering this exception.
        /// </summary>
        public ProblemSourceType ProblemSource { get; set; }

        public AzureTableDataStoreBatchedOperationException(string message, Exception inner = null) : base(message, inner)
        {
        }

        public AzureTableDataStoreBatchedOperationException(string message, ProblemSourceType problemSource, Exception inner = null) : base(message, inner)
        {
            ProblemSource = problemSource;
        }
    }

    /// <summary>
    /// Exception representing a failure of a single operation (insert, update, merge, etc).
    /// </summary>
    public class AzureTableDataStoreSingleOperationException : Exception
    {
        /// <summary>
        /// Key-Value pairs of the Exception and the entity that caused it.
        /// </summary>
        public IDictionary<Exception, object> FailedEntities { get; internal set; } = new Dictionary<Exception, object>();

        /// <summary>
        /// The problem source, indicating what was the service or reason triggering this exception.
        /// </summary>
        public ProblemSourceType ProblemSource { get; set; }

        public AzureTableDataStoreSingleOperationException(string message, Exception inner = null) : base(message, inner)
        {
            
        }

        public AzureTableDataStoreSingleOperationException(string message, ProblemSourceType problemSource, Exception inner = null) : base(message, inner)
        {
            ProblemSource = problemSource;
        }
    }
}