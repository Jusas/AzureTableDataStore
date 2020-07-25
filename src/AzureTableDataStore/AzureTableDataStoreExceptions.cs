using System;
using System.Collections.Generic;

namespace AzureTableDataStore
{
    #pragma warning disable CS1591 // Missing XML comment for publicly visible type or member
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
    /// Exception representing a failure when doing Azure Blob Storage operations.
    /// </summary>
    /// <typeparam name="TData"></typeparam>
    public class AzureTableDataStoreBlobOperationException<TData> : Exception
    {
        /// <summary>
        /// The source <see cref="LargeBlob"/> that caused the problem.
        /// </summary>
        public LargeBlob SourceBlob { get; internal set; }
        
        /// <summary>
        /// The entity whose blob the blob is.
        /// </summary>
        public TData SourceEntity { get; internal set; }

        public AzureTableDataStoreBlobOperationException(string message, TData sourceEntity = default, LargeBlob sourceBlob = null, Exception inner = null)
            : base(message, inner)
        {
            SourceEntity = sourceEntity;
            SourceBlob = sourceBlob;
        }
    }

    /// <summary>
    /// An exception context for a failed Table batch operation. Holds information of the entities
    /// that were in the operation and lists the different exceptions that occurred.
    /// </summary>
    /// <typeparam name="TData">The entity type.</typeparam>
    public class BatchExceptionContext<TData>
    {
        /// <summary>
        /// A list of any Blob Storage operation exceptions that may have occurred, with details inside each exception.
        /// </summary>
        public List<AzureTableDataStoreBlobOperationException<TData>> BlobOperationExceptions { get; } 
            = new List<AzureTableDataStoreBlobOperationException<TData>>();

        /// <summary>
        /// The list of entities in the batch that caused the exception(s).
        /// </summary>
        public List<TData> BatchEntities { get; internal set; } = new List<TData>();

        /// <summary>
        /// The Table Storage exception that may have occured. Will be null if only Blob operation exceptions occurred.
        /// </summary>
        public Exception TableOperationException { get; internal set; }

        /// <summary>
        /// The entity that was currently being handled, if it can be singled out.
        /// </summary>
        public TData CurrentEntity { get; internal set; }
    }

    /// <summary>
    /// Exception representing a failure or a number of failures that occurred inside a batch operation.
    /// Any validation exceptions will be set as a collective <see cref="AzureTableDataStoreEntityValidationException{TData}"/> in <see cref="Exception.InnerException"/>.
    /// </summary>
    public class AzureTableDataStoreBatchedOperationException<TData> : Exception
    {
        /// <summary>
        /// The exceptions collected into <see cref="BatchExceptionContext{TData}"/> defining each exception's context.
        /// </summary>
        public List<BatchExceptionContext<TData>> BatchExceptionContexts { get; internal set; } = 
            new List<BatchExceptionContext<TData>>();

        public AzureTableDataStoreBatchedOperationException(string message, Exception inner = null) : base(message, inner)
        {
        }

    }

    /// <summary>
    /// Exception representing a failure of a single operation (insert, update, merge, etc).
    /// </summary>
    public class AzureTableDataStoreSingleOperationException<TData> : Exception
    {


        /// <summary>
        /// A list of any Blob Storage operation exceptions that may have occurred, with details inside each exception.
        /// </summary>
        public List<AzureTableDataStoreBlobOperationException<TData>> BlobOperationExceptions { get; }
            = new List<AzureTableDataStoreBlobOperationException<TData>>();

        /// <summary>
        /// The entity that caused the exception.
        /// </summary>
        public TData Entity { get; internal set; }
        
        public AzureTableDataStoreSingleOperationException(string message, Exception inner = null) : base(message, inner)
        {
            
        }

    }

    /// <summary>
    /// Exception representing a failure of a multiple non-batched operations (insert, update, merge, etc).
    /// </summary>
    public class AzureTableDataStoreMultiOperationException<TData> : Exception
    {

        /// <summary>
        /// A list of single operation exceptions.
        /// </summary>
        public List<AzureTableDataStoreSingleOperationException<TData>> SingleOperationExceptions { get; internal set; } 
            = new List<AzureTableDataStoreSingleOperationException<TData>>();
        
        public AzureTableDataStoreMultiOperationException(string message, Exception inner = null) : base(message, inner)
        {
        }


    }

    /// <summary>
    /// Exception representing client side entity validation errors.
    /// </summary>
    public class AzureTableDataStoreEntityValidationException<TData> : Exception
    {

        /// <summary>
        /// The validation errors, per entity. Dictionary key is the entity itself.
        /// </summary>
        public IDictionary<TData, List<string>> EntityValidationErrors { get; } = new Dictionary<TData, List<string>>();

        public AzureTableDataStoreEntityValidationException(string message, Exception inner = null) : base(message, inner)
        {
            
        }
    }
    #pragma warning restore CS1591 // Missing XML comment for publicly visible type or member
}