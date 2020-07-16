using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs.Specialized;

[assembly: InternalsVisibleTo("AzureTableDataStore.Tests")]

namespace AzureTableDataStore
{
    /// <inheritdoc cref="ITableDataStore{TData}"/>
    public class TableDataStore<TData> : ITableDataStore<TData> where TData : new()
    {
        private class Configuration
        {
            public string BlobContainerName { get; set; }
            public PublicAccessType BlobContainerAccessType { get; set; }
            public string StorageTableName { get; set; }
            public string PartitionKeyProperty { get; set; }
            public string RowKeyProperty { get; set; }
            public bool AllowContainerCreation { get; set; }
            public bool AllowTableCreation { get; set; }
        }

        internal class EntityKeyPair
        {
            public string PartitionKey { get; set; }
            public string RowKey { get; set; }

            public EntityKeyPair(string pk, string rk)
            {
                PartitionKey = pk;
                RowKey = rk;
            }

            public EntityKeyPair()
            {
                
            }
        }

        const long MaxSingleBatchSize = 4_000_000;
        const int MaxSingleBatchCount = 100;

        private readonly object _syncLock = new object();
        public string Name { get; private set; }
        private readonly CloudStorageAccount _cloudStorageAccount;
        private readonly BlobServiceClient _blobServiceClient;
        private readonly JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings();

        private readonly Configuration _configuration;
        private bool _containerClientInitialized = false;
        private bool _tableClientInitialized = false;

        private PropertyInfo _entityTypeRowKeyPropertyInfo;
        private PropertyInfo _entityTypePartitionKeyPropertyInfo;

        private IReadOnlyCollection<ReflectionUtils.PropertyRef<LargeBlob>> _dataTypeLargeBlobRefs;
        private IReadOnlyCollection<ReflectionUtils.PropertyRef<ICollection>> _dataTypeCollectionRefs;

        private EntityPropertyConverterOptions _entityPropertyConverterOptions;
        public EntityPropertyConverterOptions EntityPropertyConverterOptions
        {
            get => _entityPropertyConverterOptions;
            set
            {
                _entityPropertyConverterOptions = value;
                _dataTypeLargeBlobRefs =
                    ReflectionUtils.GatherPropertiesWithBlobsRecursive(typeof(TData), _entityPropertyConverterOptions);
                _dataTypeCollectionRefs =
                    ReflectionUtils.GatherPropertiesWithCollectionsRecursive(typeof(TData), _entityPropertyConverterOptions);
            }
        }

        private BlobUriBuilder _blobUriBuilder;

        /// <summary>
        /// Uses client side validation if set true.
        /// <para>
        /// Client side validation runs before insert/update/merge API operations
        /// are performed and enable you to catch data issues early before trying
        /// to make actual API calls to Table and Blob Storage.
        /// </para>
        /// <para>
        /// This adds additional overhead per entity so when working with large entity
        /// numbers it may be beneficial to keep it off.
        /// </para>
        /// </summary>
        public bool UseClientSideValidation { get; set; } = false;

        /// <summary>
        /// Allows setting how many asynchronous Azure Storage Table operations (sub-batches in batch inserts/merges, or individual inserts/merges when not using batching) can be run in parallel per call.
        /// </summary>
        public int ParallelTableOperationLimit { get; set; } = 8;


        /// <summary>
        /// Allows setting how many parallel asynchronous Azure Storage Blob operations can be initiated per a sub-batch of 100 entities<br/>
        /// (so a maximum of <see cref="ParallelTableOperationLimit"/> * <see cref="ParallelBlobBatchOperationLimit"/> blob operations can be running in parallel).
        /// </summary>
        public int ParallelBlobBatchOperationLimit { get; set; } = 8;


        public TableDataStore(string tableStorageConnectionString, string tableName, bool createTableIfNotExist, string blobContainerName, bool createContainerIfNotExist, PublicAccessType blobContainerAccessType,
            string blobStorageConnectionString = null, string storeName = null, string partitionKeyProperty = null, string rowKeyProperty = null)
        {
            Name = storeName ?? "default";
            _cloudStorageAccount = CloudStorageAccount.Parse(tableStorageConnectionString);
            _blobServiceClient = new BlobServiceClient(blobStorageConnectionString ?? tableStorageConnectionString);
            _blobUriBuilder = new BlobUriBuilder(blobStorageConnectionString ?? tableStorageConnectionString);
            _configuration = new Configuration()
            {
                BlobContainerAccessType = blobContainerAccessType,
                BlobContainerName = blobContainerName,
                StorageTableName = tableName,
                PartitionKeyProperty = ResolvePartitionKeyProperty(partitionKeyProperty),
                RowKeyProperty = ResolveRowKeyProperty(rowKeyProperty),
                AllowContainerCreation = createContainerIfNotExist,
                AllowTableCreation = createTableIfNotExist
            };
            PostConfigure();
        }

        public TableDataStore(StorageCredentials tableStorageCredentials, StorageUri tableStorageUri, string tableName, bool createTableIfNotExist,
            StorageSharedKeyCredential blobStorageCredentials, Uri blobStorageServiceUri, string blobContainerName, bool createContainerIfNotExist, PublicAccessType blobContainerAccessType,
            string storeName = null, string partitionKeyProperty = null, string rowKeyProperty = null)
        {
            Name = storeName ?? "default";
            _cloudStorageAccount = new CloudStorageAccount(tableStorageCredentials, tableStorageUri);
            _blobServiceClient = new BlobServiceClient(blobStorageServiceUri, blobStorageCredentials);
            _blobUriBuilder = new BlobUriBuilder(blobStorageCredentials, blobStorageServiceUri);
            _configuration = new Configuration()
            {
                BlobContainerAccessType = blobContainerAccessType,
                BlobContainerName = blobContainerName,
                StorageTableName = tableName,
                PartitionKeyProperty = ResolvePartitionKeyProperty(partitionKeyProperty),
                RowKeyProperty = ResolveRowKeyProperty(rowKeyProperty),
                AllowContainerCreation = createContainerIfNotExist,
                AllowTableCreation = createTableIfNotExist
            };
            PostConfigure();
        }

        private void PostConfigure()
        {
            _entityTypeRowKeyPropertyInfo = typeof(TData).GetProperty(_configuration.RowKeyProperty);
            _entityTypePartitionKeyPropertyInfo = typeof(TData).GetProperty(_configuration.PartitionKeyProperty);

            EntityPropertyConverterOptions = new EntityPropertyConverterOptions();
        }

        private string ResolvePartitionKeyProperty(string inputPartitionKeyProperty)
        {
            var entryType = typeof(TData);
            var properties = entryType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            if (!string.IsNullOrEmpty(inputPartitionKeyProperty))
            {
                if (properties.All(x => x.Name != inputPartitionKeyProperty))
                    throw new AzureTableDataStoreConfigurationException($"Given partition key property name '{inputPartitionKeyProperty}' " +
                        $"is not a property in the data type '{entryType.Name}', please specify a valid property to act as partition key!");

                return inputPartitionKeyProperty;
            }

            var partitionKeyProperty = properties.FirstOrDefault(x => x.GetCustomAttributes(typeof(TablePartitionKeyAttribute)).Any());
            if (partitionKeyProperty != null)
                return partitionKeyProperty.Name;

            throw new AzureTableDataStoreConfigurationException($"Unable to resolve partition key for Type '{entryType.Name}', " +
                $"no explicit partition key was provided in {nameof(TableDataStore<TData>)} constructor and the Type has " +
                $"no property with the '{nameof(TablePartitionKeyAttribute)}' attribute.");
        }

        private string ResolveRowKeyProperty(string inputRowKeyProperty)
        {
            var entryType = typeof(TData);
            var properties = entryType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            if (!string.IsNullOrEmpty(inputRowKeyProperty))
            {
                if (properties.All(x => x.Name != inputRowKeyProperty))
                    throw new AzureTableDataStoreConfigurationException($"Given row key property name '{inputRowKeyProperty}' " +
                        $"is not a property in the data type '{entryType.Name}', please specify a valid property to act as row key!");

                return inputRowKeyProperty;
            }

            var rowKeyProperty = properties.FirstOrDefault(x => x.GetCustomAttributes(typeof(TableRowKeyAttribute)).Any());
            if (rowKeyProperty != null)
                return rowKeyProperty.Name;

            throw new AzureTableDataStoreConfigurationException($"Unable to resolve row key for Type '{entryType.Name}', " +
                $"no explicit row key was provided in {nameof(TableDataStore<TData>)} constructor and the Type has " +
                $"no property with the '{nameof(TableRowKeyAttribute)}' attribute.");
        }

        private (string partitionKey, string rowKey) GetEntityKeys(TData entry)
        {
            return (_entityTypePartitionKeyPropertyInfo.GetValue(entry).ToString(), _entityTypeRowKeyPropertyInfo.GetValue(entry).ToString());
        }

        public async Task InsertAsync(BatchingMode batchingMode, params TData[] entities)
        {
            if (entities == null || entities.Length == 0)
                return;

            await InsertInternalAsync(entities, batchingMode, false);
        }

        private BlobContainerClient GetContainerClient()
        {
            lock (_syncLock)
            {
                if (!_containerClientInitialized)
                {
                    if (_configuration.AllowContainerCreation)
                    {
                        try
                        {
                            _blobServiceClient
                                .GetBlobContainerClient(_configuration.BlobContainerName)
                                .CreateIfNotExists(_configuration.BlobContainerAccessType);
                            _containerClientInitialized = true;
                        }
                        catch (Exception e)
                        {
                            throw new AzureTableDataStoreInternalException(
                                "Unable to initialize blob container (CreateIfNotExists): " + e.Message,
                                e);
                        }
                    }
                    else
                    {
                        try
                        {
                            var exists = _blobServiceClient
                                .GetBlobContainerClient(_configuration.BlobContainerName)
                                .Exists();

                            if (!exists)
                                throw new AzureTableDataStoreInternalException(
                                    $"Blob container '{_configuration.BlobContainerName}' does not exist");

                            _containerClientInitialized = true;
                        }
                        catch (AzureTableDataStoreInternalException)
                        {
                            throw;
                        }
                        catch (Exception e)
                        {
                            throw new AzureTableDataStoreInternalException("Unable to initialize blob container: " + e.Message, e);
                        }
                    }
                }
            }

            var blobContainerClient = _blobServiceClient.GetBlobContainerClient(_configuration.BlobContainerName);
            return blobContainerClient;
        }

        private CloudTable GetTable()
        {
            lock (_syncLock)
            {
                if (!_tableClientInitialized)
                {
                    if (_configuration.AllowTableCreation)
                    {
                        try
                        {
                            var cloudTableClient = _cloudStorageAccount.CreateCloudTableClient();
                            var tableRef = cloudTableClient.GetTableReference(_configuration.StorageTableName);
                            tableRef.CreateIfNotExists();
                            _tableClientInitialized = true;
                            return tableRef;
                        }
                        catch (Exception e)
                        {
                            throw new AzureTableDataStoreInternalException("Unable to initialize table (CreateIfNotExists): " + e.Message,
                                e);
                        }
                    }
                    else
                    {
                        try
                        {
                            var cloudTableClient = _cloudStorageAccount.CreateCloudTableClient();
                            var tableRef = cloudTableClient.GetTableReference(_configuration.StorageTableName);
                            bool exists = tableRef.Exists();
                            if(!exists)
                                throw new AzureTableDataStoreInternalException(
                                    $"Table '{_configuration.StorageTableName}' does not exist");

                            _tableClientInitialized = true;
                            return tableRef;
                        }
                        catch (Exception e)
                        {
                            throw new AzureTableDataStoreInternalException("Unable to initialize table (CreateIfNotExists): " + e.Message,
                                e);
                        }
                    }
                
                }
         
            }

            return _cloudStorageAccount.CreateCloudTableClient()
                .GetTableReference(_configuration.StorageTableName);
        }

        private void StripSpeciallyHandledProperties(IEnumerable<ReflectionUtils.PropertyRef> propertyRefs)
        {
            // Set these values to null to not attempt their serialization with EntityPropertyConverter.
            // Otherwise the conversion will be attempted, and an exception thrown.
            // We will set them back to what they were after we've performed the serialization.
            // It's not nice, but I can live with it.

            foreach (var propRef in propertyRefs)
                propRef.Property.SetValue(propRef.SourceObject, null);
        }

        private void RestoreSpeciallyHandledProperties(IEnumerable<ReflectionUtils.PropertyRef> propertyRefs)
        {
            // Restore the specially serialized values back in their place.

            foreach (var propRef in propertyRefs)
                propRef.Property.SetValue(propRef.SourceObject, propRef.StoredInstanceAsObject);
        }


        
        private string BuildBlobPath(ReflectionUtils.PropertyRef<LargeBlob> blobPropRef, string partitionKey,
            string rowKey)
        {
            return BuildBlobPath(_configuration.StorageTableName, partitionKey, rowKey,
                blobPropRef.FlattenedPropertyName, blobPropRef.StoredInstance?.Filename ?? "");
        }

        internal string BuildBlobPath(string storageTableName, string partitionKey, string rowKey,
            string flattenedPropertyName, string filename)
        {
            return string.Join("/",
                _configuration.StorageTableName,
                partitionKey,
                rowKey,
                flattenedPropertyName,
                filename);
        }

        private async Task DeleteBlobsFromReference(TData sourceEntity, ReflectionUtils.PropertyRef<LargeBlob> blobPropRef,
            string blobPath)
        {
            try
            {
                var containerClient = GetContainerClient();
                var existingBlobPaths = new List<string>();

                var existingBlobs =
                    containerClient.GetBlobsAsync(prefix: blobPath.Substring(0, blobPath.LastIndexOf('/')));
                var enumerator = existingBlobs.GetAsyncEnumerator();
                while (await enumerator.MoveNextAsync())
                {
                    var existingBlob = enumerator.Current;
                    existingBlobPaths.Add(existingBlob.Name);
                }

                foreach (var existingBlob in existingBlobPaths)
                {
                    await containerClient.GetBlobClient(existingBlob).DeleteIfExistsAsync();
                }
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreBlobOperationException<TData>("Blob operation failed: " + e.Message,
                    sourceEntity, blobPropRef.StoredInstance, e);
            }
        }

        private async Task HandleBlobAndUpdateReference(TData sourceEntity, ReflectionUtils.PropertyRef<LargeBlob> blobPropRef, string blobPath, bool allowReplace, LargeBlobNullBehavior largeBlobNullBehavior)
        {
            try
            {
                var containerClient = GetContainerClient();
                var blobClient = containerClient.GetBlobClient(blobPath);

                // In case we're setting purposefully a blob to null with the intention of deleting it, or we're replacing the blob with a new one,
                // we need to collect the old blob instances to a list so that we can delete them.

                var oldBlobPaths = new List<string>();

                if (blobPropRef.StoredInstance == null && largeBlobNullBehavior == LargeBlobNullBehavior.DeleteBlob)
                {
                    var oldBlobs =
                        containerClient.GetBlobsAsync(prefix: blobPath.Substring(0, blobPath.LastIndexOf('/')));
                    var enumerator = oldBlobs.GetAsyncEnumerator();
                    while (await enumerator.MoveNextAsync())
                    {
                        var oldBlob = enumerator.Current;
                        oldBlobPaths.Add(oldBlob.Name);
                    }
                }

                if (blobPropRef.StoredInstance == null && largeBlobNullBehavior == LargeBlobNullBehavior.IgnoreProperty)
                    return;


                if (blobPropRef.StoredInstance == null && largeBlobNullBehavior == LargeBlobNullBehavior.DeleteBlob)
                {
                    foreach (var oldBlob in oldBlobPaths)
                    {
                        await containerClient.GetBlobClient(oldBlob).DeleteIfExistsAsync();
                    }

                    return;
                }


                var dataStream = await blobPropRef.StoredInstance.AsyncDataStream.Value;
                await blobClient.UploadAsync(dataStream, new BlobHttpHeaders()
                {
                    ContentType = blobPropRef.StoredInstance.ContentType
                }, conditions: allowReplace ? null : new BlobRequestConditions {IfNoneMatch = new ETag("*")});
                // Should we compare the hashes just in case?
                dataStream.Seek(0, SeekOrigin.Begin);
                var props = await blobClient.GetPropertiesAsync();
                blobPropRef.StoredInstance.Length = props.Value.ContentLength;
                blobPropRef.StoredInstance.ContentType = props.Value.ContentType;
                blobPropRef.StoredInstance.BlobClient = blobClient;
                blobPropRef.StoredInstance.BlobUriBuilder = _blobUriBuilder;

                // Delete the old blob(s). These will exist if the blob's filename changed, so we need to remove them.

                foreach (var oldBlob in oldBlobPaths)
                {
                    if (oldBlob == blobPath)
                        continue;

                    await containerClient.GetBlobClient(oldBlob).DeleteIfExistsAsync();
                }
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreBlobOperationException<TData>("Blob operation failed: " + e.Message,
                    sourceEntity, blobPropRef.StoredInstance, e);
            }
            
        }

        private async Task<Stream> GetBlobStreamFromAzureBlobStorage(TData entity, LargeBlob sourceLargeBlob, string blobPath)
        {
            try
            {
                var containerClient = GetContainerClient();
                var blobClient = containerClient.GetBlobClient(blobPath);

                var downloadRequestResult = await blobClient.DownloadAsync();
                return downloadRequestResult.Value.Content;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreBlobOperationException<TData>($"Failed to initiate blob download for blob '{blobPath}': " + e.Message,
                    entity, sourceLargeBlob, e);
            }
        }


        private class BatchItem
        {
            public DynamicTableEntity SerializedEntity { get; set; }
            public TData SourceEntity { get; set; }
            public List<ReflectionUtils.PropertyRef<LargeBlob>> LargeBlobRefs { get; set; }
        }
        

        private async Task InsertInternalAsync(TData[] entities, BatchingMode batchingMode, bool allowReplace)
        {
            
            // Run some pre-insert checks and throw early on errors.

            ValidateBatchParameters(entities, batchingMode, false);
            
            var entityPartitionGroups = entities.GroupBy(x => _entityTypePartitionKeyPropertyInfo.GetValue(x))
                .ToDictionary(x => (string)x.Key, x => x.ToList());


            var entityBatches = new List<List<BatchItem>>();
            var validationException = new AzureTableDataStoreEntityValidationException<TData>("Client side validation failed for some entities. "
                + $"See {nameof(AzureTableDataStoreEntityValidationException<TData>.EntityValidationErrors)} for details.");


            // Collect entities into batches for insert/replace.
            // Throw if we get errors.

            TData currentEntity = default;
            var allEntities = new List<BatchItem>();

            try
            {

                foreach (var group in entityPartitionGroups)
                {
                    List<BatchItem> entityBatch = null;

                    foreach (var entity in group.Value)
                    {
                        currentEntity = entity;
                        var entityKeys = GetEntityKeys(entity);
                        var entityData = ExtractEntityProperties(entity, allowReplace);

                        entityData.CollectionPropertyRefs.ForEach(@ref =>
                            entityData.PropertyDictionary.Add(@ref.FlattenedPropertyName, EntityProperty.GeneratePropertyForString(
                                JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                        foreach (var @ref in entityData.BlobPropertyRefs)
                        {
                            if (@ref.StoredInstance == null)
                                continue;

                            var stream = await @ref.StoredInstance.AsyncDataStream.Value;
                            @ref.StoredInstance.Length = stream.Length;
                            entityData.PropertyDictionary.Add(@ref.FlattenedPropertyName,
                                EntityProperty.GeneratePropertyForString(
                                    JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings)));
                        }

                        var tableEntity = new DynamicTableEntity()
                        {
                            PartitionKey = entityKeys.partitionKey,
                            RowKey = entityKeys.rowKey,
                            ETag = "*",
                            Properties = entityData.PropertyDictionary
                        };


                        if (UseClientSideValidation)
                        {
                            var entityValidationErrors = SerializationUtils.ValidateProperties(tableEntity);
                            var blobPaths = entityData.BlobPropertyRefs.Select(x =>
                                BuildBlobPath(x, entityKeys.partitionKey, entityKeys.rowKey));
                            var pathValidations = blobPaths.Select(x => SerializationUtils.ValidateBlobPath(x));
                            entityValidationErrors.AddRange(pathValidations.Where(x => x.Count > 0).SelectMany(x => x));

                            if (entityValidationErrors.Any())
                            {
                                validationException.EntityValidationErrors.Add(entity, entityValidationErrors);
                            }
                        }

                        if (batchingMode == BatchingMode.None)
                        {
                            allEntities.Add(new BatchItem()
                            {
                                SourceEntity = entity,
                                SerializedEntity = tableEntity,
                                LargeBlobRefs = entityData.BlobPropertyRefs
                            });
                            continue;
                        }


                        if (entityBatch == null)
                        {
                            entityBatch = new List<BatchItem>();
                            entityBatches.Add(entityBatch);
                        }

                        var batchSize = SerializationUtils.CalculateApproximateBatchRequestSize(
                            entityBatch.Select(x => x.SerializedEntity).Append(tableEntity).ToArray());
                        
                        if (batchSize < MaxSingleBatchSize && entityBatch.Count < MaxSingleBatchCount)
                        {
                            entityBatch.Add(new BatchItem()
                            {
                                SourceEntity = entity,
                                SerializedEntity = tableEntity,
                                LargeBlobRefs = entityData.BlobPropertyRefs
                            });
                        }
                        else
                        {
                            // Strict mode means a single batch transaction, so we'll just throw if it doesn't fit into one batch.
                            if (batchingMode == BatchingMode.Strict)
                            {
                                throw new AzureTableDataStoreInternalException("Entities do not fit into a single batch, unable to insert with BatchingMode.Strict");
                            }
                            entityBatch = new List<BatchItem>
                            {
                                new BatchItem()
                                {
                                    SourceEntity = entity,
                                    SerializedEntity = tableEntity,
                                    LargeBlobRefs = entityData.BlobPropertyRefs
                                }
                            };
                            entityBatches.Add(entityBatch);
                        }
                    }
                }

                if (validationException.EntityValidationErrors.Any())
                    throw validationException;
            }
            catch (AzureTableDataStoreEntityValidationException<TData>)
            {
                throw;
            }
            catch (SerializationException e)
            {
                if (batchingMode == BatchingMode.None && entities.Length < 2)
                {
                    throw new AzureTableDataStoreSingleOperationException<TData>(
                        "Serialization of the data failed: " + e.Message, e)
                    {
                        Entity = entities[0]
                    };
                }

                if (batchingMode == BatchingMode.None)
                {
                    throw new AzureTableDataStoreMultiOperationException<TData>(
                        "Serialization of the data failed: " + e.Message, e)
                    {
                        SingleOperationExceptions = new List<AzureTableDataStoreSingleOperationException<TData>>()
                        {
                            new AzureTableDataStoreSingleOperationException<TData>("Serialization error: " + e.Message, e)
                            {
                                Entity = currentEntity
                            }
                        }
                    };
                }

                var ex = new AzureTableDataStoreBatchedOperationException<TData>(
                    "Serialization of the data failed: " + e.Message, e);
                ex.BatchExceptionContexts.Add(new BatchExceptionContext<TData>()
                {
                    BatchEntities = entities.ToList(),
                    CurrentEntity = currentEntity
                });
                throw ex;
            }
            catch (Exception e)
            {
                if (batchingMode == BatchingMode.None && entities.Length < 2)
                {
                    throw new AzureTableDataStoreSingleOperationException<TData>(
                        "Failed to prepare the entity for insert/replace: " + e.Message, e)
                    {
                        Entity = entities[0]
                    };
                }

                if (batchingMode == BatchingMode.None)
                {
                    throw new AzureTableDataStoreMultiOperationException<TData>(
                        "Failed to prepare the entities for insert/replace: " + e.Message, e)
                    {
                        SingleOperationExceptions = new List<AzureTableDataStoreSingleOperationException<TData>>()
                        {
                            new AzureTableDataStoreSingleOperationException<TData>("Failed to prepare entity for insert/replace: " + e.Message, e)
                            {
                                Entity = currentEntity
                            }
                        }
                    };
                }

                var ex = new AzureTableDataStoreBatchedOperationException<TData>(
                    "Failed to group entities into batches: " + e.Message, e);
                ex.BatchExceptionContexts.Add(new BatchExceptionContext<TData>()
                {
                    BatchEntities = entities.ToList()
                });
                throw ex;
            }


            // Run the prepared batches/items.


            if (batchingMode == BatchingMode.None)
            {
                var failedOps = new ConcurrentBag<AzureTableDataStoreSingleOperationException<TData>>();
                try
                {
                    var parallelOpGroups = ArrayExtensions.SplitToBatches(allEntities, ParallelTableOperationLimit);
                    foreach (var parallelOpGroup in parallelOpGroups)
                    {
                        var parallelOpsAsTasks = parallelOpGroup.Select(
                            item => RunAsSingleOperation(allowReplace, allowReplace ? TableOperationType.InsertOrReplace : TableOperationType.Insert,
                                LargeBlobNullBehavior.DeleteBlob, item, failedOps));
                        var parallelTaskRuns = Task.WhenAll(parallelOpsAsTasks);
                        await parallelTaskRuns;
                    }

                    if (failedOps.Count > 1)
                    {
                        var exception = new AzureTableDataStoreMultiOperationException<TData>(
                            "One or more operations had errors");
                        exception.SingleOperationExceptions.AddRange(failedOps);
                        throw exception;
                    }

                    if (failedOps.Count == 1 && allEntities.Count == 1)
                    {
                        failedOps.TryTake(out var ex);
                        throw ex;
                    }
                }
                catch (AzureTableDataStoreSingleOperationException<TData>)
                {
                    throw;
                }
                catch (AzureTableDataStoreMultiOperationException<TData>)
                {
                    throw;
                }
                catch (Exception e) when (allEntities.Count > 1)
                {
                    var ex = new AzureTableDataStoreMultiOperationException<TData>(
                        $"Unexpected exception in multi-operation execution, see inner exception: " + e.Message, e);
                    ex.SingleOperationExceptions.AddRange(failedOps);
                    throw ex;
                }
                catch (Exception e) when (allEntities.Count == 1)
                {
                    var ex = new AzureTableDataStoreSingleOperationException<TData>(
                        $"Unexpected exception in single operation execution, see inner exception: " + e.Message, e);
                    ex.Entity = allEntities[0].SourceEntity;
                    throw ex;
                }

                return;
            }

            try
            {

                var failedTableBatches = new ConcurrentBag<BatchExceptionContext<TData>>();

                var parallelBatchGroups = ArrayExtensions.SplitToBatches(entityBatches, ParallelTableOperationLimit);
                foreach (var batchGroup in parallelBatchGroups)
                {
                    var batchGroupAsTasks = batchGroup.Select(
                        batchItems => RunAsBatchOperations(allowReplace, batchingMode, 
                            allowReplace ? TableOperationType.InsertOrReplace : TableOperationType.Insert, 
                            LargeBlobNullBehavior.DeleteBlob, batchItems, failedTableBatches));
                    var parallelTaskRuns = Task.WhenAll(batchGroupAsTasks);
                    await parallelTaskRuns;
                
                }

                // if any failed table batches or blob uploads, throw a collective AzureTableDataStoreBatchedOperationException
                if (failedTableBatches.Any())
                {
                    string exceptionMessage = "One or more Table insert/replace batch calls failed, see BatchExceptionContexts for more information.";
                    throw new AzureTableDataStoreBatchedOperationException<TData>(exceptionMessage)
                    {
                        BatchExceptionContexts = failedTableBatches.ToList()
                    };
                }

            }
            catch (AzureTableDataStoreBatchedOperationException<TData>)
            {
                throw;
            }
            catch (Exception e)
            {
                var ex = new AzureTableDataStoreBatchedOperationException<TData>(
                    $"Unexpected exception in batch insert/replace: " + e.Message,
                    e);
                ex.BatchExceptionContexts.Add(new BatchExceptionContext<TData>()
                {
                    BatchEntities = entities.ToList()
                });
                throw ex;
            }

        }


        private void ValidateBatchParameters(IList<TData> entities, BatchingMode batchingMode, bool disallowLargeBlobsInStrictAndStrongMode, LargeBlobNullBehavior? largeBlobNullBehavior = null)
        {
            try
            {
                if (entities.Count > MaxSingleBatchCount && batchingMode == BatchingMode.Strict)
                {
                    throw new AzureTableDataStoreInternalException(
                        $"Strict batching mode cannot handle more than {MaxSingleBatchCount} entities, which is the limit imposed by Azure Table Storage");
                }

                // For Strict and Strong batching modes:
                // If the data type has blob properties, we need to check that all of the entities have them set as null
                // because we do not support storage blob inserts in those modes. The only batching mode that supports
                // blob operations is BatchingMode.Loose.

                var blobProperties = _dataTypeLargeBlobRefs;

                if ((batchingMode == BatchingMode.Strict || batchingMode == BatchingMode.Strong) && blobProperties.Any())
                {
                    if(disallowLargeBlobsInStrictAndStrongMode)
                        throw new AzureTableDataStoreInternalException("Strict and Strong batching modes cannot be used with entities that have LargeBlob properties.");

                    foreach (var entity in entities)
                    {
                        var blobProps =
                            ReflectionUtils.GatherPropertiesWithBlobsRecursive(entity,
                                EntityPropertyConverterOptions);
                        foreach (var bp in blobProps)
                        {
                            if (bp.StoredInstance != null)
                                throw new AzureTableDataStoreInternalException(
                                    "Batched operations are not supported for entity types with LargeBlob properties due to the " +
                                    "transactional nature of Table batch inserts. Please use either BatchingMode.Loose or BatchingMode.None when inserting LargeBlobs.");
                        }

                    }
                }


                var partitionCount = entities
                    .Select(x => (string) _entityTypePartitionKeyPropertyInfo.GetValue(x))
                    .Distinct()
                    .Count();

                // Again, if using Strict batching mode, we may only have entities from the same partition.

                if (batchingMode == BatchingMode.Strict && partitionCount > 1)
                {
                    throw new AzureTableDataStoreInternalException(
                        "Strict batching mode requires all entities to have the same partition key.");
                }

                // The idea of trying to delete blobs when LargeBlobs are set to null is incompatible with Strict
                // and Strong batching modes. No blob operations whatsoever in these modes.

                if ((batchingMode != BatchingMode.Loose && batchingMode != BatchingMode.None) && largeBlobNullBehavior.HasValue && largeBlobNullBehavior != LargeBlobNullBehavior.IgnoreProperty)
                {
                    throw new AzureTableDataStoreInternalException(
                        "Strict and Strong batching modes cannot perform any Blob operations, " +
                        "largeBlobNullBehavior must be set to IgnoreProperty with these batching modes.");
                }
                
            }
            catch (Exception e)
            {
                var ex = new AzureTableDataStoreBatchedOperationException<TData>(
                    "Pre-checks for batch operation failed: " + e.Message,
                    e);
                ex.BatchExceptionContexts.Add(new BatchExceptionContext<TData>()
                {
                    BatchEntities = entities.ToList()
                });
                throw ex;
            }
        }

        private TData CreateEntityFromKeys(EntityKeyPair keypair)
        {
            var entity = new TData();
            _entityTypePartitionKeyPropertyInfo.SetValue(entity, keypair.PartitionKey);
            _entityTypeRowKeyPropertyInfo.SetValue(entity, keypair.RowKey);
            return entity;
        }

        // Note: largeBlobNullBehavior does not matter when opType is delete.
        private async Task RunAsSingleOperation(bool allowReplace, TableOperationType opType, LargeBlobNullBehavior largeBlobNullBehavior, BatchItem entityItem, 
            ConcurrentBag<AzureTableDataStoreSingleOperationException<TData>> failedOps)
        {
            try
            {
                var tableRef = GetTable();
                var tableOp = TableWriteOpFromType(opType, entityItem.SerializedEntity);

                await tableRef.ExecuteAsync(tableOp);
            }
            catch (Exception e)
            {
                var exception = new AzureTableDataStoreSingleOperationException<TData>(
                    $"Failed '{opType}' Table Storage operation: " + e.Message, e)
                {
                    Entity = entityItem.SourceEntity
                };
                failedOps.Add(exception);
                return;
            }

            // Upload files after the table insert is successful - otherwise there's no point.
            Task collectiveBlobOpTask = null;
            try
            {

                if (opType == TableOperationType.Delete)
                {
                    var blobOpTasks = entityItem.LargeBlobRefs
                        .Select(x => DeleteBlobsFromReference(entityItem.SourceEntity, x,
                            BuildBlobPath(x, entityItem.SerializedEntity.PartitionKey, entityItem.SerializedEntity.RowKey))).ToArray();
                    collectiveBlobOpTask = Task.WhenAll(blobOpTasks);
                    await collectiveBlobOpTask;
                }
                else
                {
                    var blobOpTasks = entityItem.LargeBlobRefs
                        .Select(x => HandleBlobAndUpdateReference(entityItem.SourceEntity, x,
                            BuildBlobPath(x, entityItem.SerializedEntity.PartitionKey, entityItem.SerializedEntity.RowKey), allowReplace,
                            largeBlobNullBehavior)).ToArray();
                    collectiveBlobOpTask = Task.WhenAll(blobOpTasks);
                    await collectiveBlobOpTask;
                }
                
            }
            catch (Exception e)
            {
                var exception = new AzureTableDataStoreSingleOperationException<TData>(
                    $"One or more Blob Storage operations failed. See {nameof(AzureTableDataStoreSingleOperationException<TData>.BlobOperationExceptions)} for details.",
                    inner: e)
                {
                    Entity = entityItem.SourceEntity
                };

                foreach (var inner in collectiveBlobOpTask.Exception.InnerExceptions)
                {
                    exception.BlobOperationExceptions.Add(
                        inner as AzureTableDataStoreBlobOperationException<TData>);
                }

                failedOps.Add(exception);
            }

        }

        private TableOperation TableWriteOpFromType(TableOperationType type, DynamicTableEntity entity)
        {
            switch (type)
            {
                case TableOperationType.Insert:
                    return TableOperation.Insert(entity);
                case TableOperationType.InsertOrReplace:
                    return TableOperation.InsertOrReplace(entity);
                case TableOperationType.Delete:
                    return TableOperation.Delete(entity);
                case TableOperationType.InsertOrMerge:
                    return TableOperation.InsertOrMerge(entity);
                case TableOperationType.Merge:
                    return TableOperation.Merge(entity);
                case TableOperationType.Replace:
                    return TableOperation.Replace(entity);
            }
            throw new Exception("Unsupported op type");
        }

        // A single batch insert/replace operation.
        // Note: largeBlobNullBehavior does not matter when opType is delete.
        private async Task RunAsBatchOperations(bool allowReplace, BatchingMode batchingMode, TableOperationType opType, LargeBlobNullBehavior largeBlobNullBehavior, 
            List<BatchItem> batchItems, ConcurrentBag<BatchExceptionContext<TData>> failedTableBatches)
        {
            // Attempt table op first, and proceed with related blob ops if the table op succeeds.

            var batchExceptionContext = new BatchExceptionContext<TData>();
            batchExceptionContext.BatchEntities = batchItems.Select(x => x.SourceEntity).ToList();

            try
            {
                var tableRef = GetTable();
                var batchOp = new TableBatchOperation();

                batchItems.ForEach(item => batchOp.Add(TableWriteOpFromType(opType, item.SerializedEntity)));
                await tableRef.ExecuteBatchAsync(batchOp);
            }
            catch (Exception e)
            {
                batchExceptionContext.TableOperationException = e;
                failedTableBatches.Add(batchExceptionContext);
                return;
            }

            if (batchingMode != BatchingMode.Loose)
                return;

            // Handle blob ops with some degree of parallelism.
            // Collect failed blob operations to a list, later to be stored into an aggregate exception.
            try
            {
                var flattenedUploadList =
                    new List<(TData sourceEntity, DynamicTableEntity serializedEntity,
                        ReflectionUtils.PropertyRef<LargeBlob> blobRef)>();
                batchItems.ForEach(x =>
                    x.LargeBlobRefs.ForEach(r =>
                        flattenedUploadList.Add((x.SourceEntity, x.SerializedEntity, r))));

                var parallelBlobOpGroups = ArrayExtensions.SplitToBatches(flattenedUploadList, ParallelBlobBatchOperationLimit);

                foreach (var parallelBlobOpGroup in parallelBlobOpGroups)
                {
                    Task collectiveBlobOpTask = null;
                    try
                    {
                        var parallelBlobOpTasks = parallelBlobOpGroup.Select(blobOp =>
                        {
                            var pk = blobOp.serializedEntity.PartitionKey;
                            var rk = blobOp.serializedEntity.RowKey;

                            if (opType == TableOperationType.Delete)
                                return DeleteBlobsFromReference(blobOp.sourceEntity, blobOp.blobRef,
                                    BuildBlobPath(blobOp.blobRef, pk, rk));

                            return HandleBlobAndUpdateReference(blobOp.sourceEntity, blobOp.blobRef,
                                BuildBlobPath(blobOp.blobRef, pk, rk),
                                allowReplace, largeBlobNullBehavior);
                        });

                        collectiveBlobOpTask = Task.WhenAll(parallelBlobOpTasks);
                        await collectiveBlobOpTask;
                    }
                    catch (Exception)
                    {
                        foreach (var ex in collectiveBlobOpTask.Exception.InnerExceptions)
                        {
                            if (ex is AzureTableDataStoreBlobOperationException<TData> blobEx)
                                batchExceptionContext.BlobOperationExceptions.Add(blobEx);
                            else
                                batchExceptionContext.BlobOperationExceptions.Add(
                                    new AzureTableDataStoreBlobOperationException<TData>(ex.Message,
                                        inner: ex));

                        }
                        failedTableBatches.Add(batchExceptionContext);
                    }
                }

            }
            catch (Exception e)
            {
                var unexpected =
                    new AzureTableDataStoreBlobOperationException<TData>("Unhandled exception: " + e.Message,
                        inner: e);
                batchExceptionContext.BlobOperationExceptions.Add(unexpected);
                failedTableBatches.Add(batchExceptionContext);
            }
        }


        public async Task DeleteAsync(BatchingMode batchingMode, params TData[] entities)
        {
            if (entities == null || entities.Length == 0)
                return;

            await DeleteInternalAsync(batchingMode, entities);
        }

        public async Task DeleteAsync(BatchingMode batchingMode,
            params (string partitionKey, string rowKey)[] entityIds)
        {
            if (entityIds == null || entityIds.Length == 0)
                return;

            await DeleteInternalAsync(batchingMode,
                entityIds.Select(x => new EntityKeyPair(x.partitionKey, x.rowKey)).ToArray());
        }

        public async Task DeleteAsync(BatchingMode batchingMode, Expression<Func<TData, bool>> queryExpression)
        {
            var results = await FindAsync(queryExpression, x => new {PartitionKey = "", RowKey = ""});

            await DeleteInternalAsync(batchingMode, results.ToArray());
        }

        private async Task DeleteInternalAsync(BatchingMode batchingMode, TData[] entities)
        {

            ValidateBatchParameters(entities, batchingMode, true);

            var entityPartitionGroups = entities.GroupBy(x => _entityTypePartitionKeyPropertyInfo.GetValue(x))
                .ToDictionary(x => (string)x.Key, x => x.ToList());

            var entityBatches = new List<List<BatchItem>>();
            var validationException = new AzureTableDataStoreEntityValidationException<TData>("Client side validation failed for some entities. "
                + $"See {nameof(AzureTableDataStoreEntityValidationException<TData>.EntityValidationErrors)} for details.");


            // Collect entities into batches for delete.
            // Throw if we get errors.

            TData currentEntity = default;
            var allEntities = new List<BatchItem>();

            try
            {

                foreach (var group in entityPartitionGroups)
                {
                    List<BatchItem> entityBatch = null;

                    foreach (var entity in group.Value)
                    {
                        currentEntity = entity;
                        var entityKeys = GetEntityKeys(entity);
                        var entityData = ExtractEntityProperties(entity, true);

                        var tableEntity = new DynamicTableEntity(entityKeys.partitionKey, entityKeys.rowKey)
                        {
                            ETag = "*"
                        };

                        if (UseClientSideValidation)
                        {
                            var entityValidationErrors = SerializationUtils.ValidateProperties(tableEntity);
                            
                            if (entityValidationErrors.Any())
                            {
                                validationException.EntityValidationErrors.Add(entity, entityValidationErrors);
                            }
                        }

                        if (batchingMode == BatchingMode.None)
                        {
                            allEntities.Add(new BatchItem()
                            {
                                SourceEntity = entity,
                                SerializedEntity = tableEntity,
                                LargeBlobRefs = entityData.BlobPropertyRefs
                            });
                            continue;
                        }


                        if (entityBatch == null)
                        {
                            entityBatch = new List<BatchItem>();
                            entityBatches.Add(entityBatch);
                        }

                        var batchSize = SerializationUtils.CalculateApproximateBatchRequestSize(
                            entityBatch.Select(x => x.SerializedEntity).Append(tableEntity).ToArray());

                        if (batchSize < MaxSingleBatchSize && entityBatch.Count < MaxSingleBatchCount)
                        {
                            entityBatch.Add(new BatchItem()
                            {
                                SourceEntity = entity,
                                SerializedEntity = tableEntity,
                                LargeBlobRefs = entityData.BlobPropertyRefs
                            });
                        }
                        else
                        {
                            // Strict mode means a single batch transaction, so we'll just throw if it doesn't fit into one batch.
                            if (batchingMode == BatchingMode.Strict)
                            {
                                throw new AzureTableDataStoreInternalException("Entities do not fit into a single batch, unable to insert with BatchingMode.Strict");
                            }
                            entityBatch = new List<BatchItem>
                            {
                                new BatchItem()
                                {
                                    SourceEntity = entity,
                                    SerializedEntity = tableEntity,
                                    LargeBlobRefs = entityData.BlobPropertyRefs
                                }
                            };
                            entityBatches.Add(entityBatch);
                        }
                    }
                }

                if (validationException.EntityValidationErrors.Any())
                    throw validationException;
            }
            catch (AzureTableDataStoreEntityValidationException<TData>)
            {
                throw;
            }
            catch (Exception e)
            {
                if (batchingMode == BatchingMode.None && entities.Length < 2)
                {
                    throw new AzureTableDataStoreSingleOperationException<TData>(
                        "Failed to prepare the entity for deletion: " + e.Message, e)
                    {
                        Entity = entities[0]
                    };
                }

                if (batchingMode == BatchingMode.None)
                {
                    throw new AzureTableDataStoreMultiOperationException<TData>(
                        "Failed to prepare the entities for deletion: " + e.Message, e)
                    {
                        SingleOperationExceptions = new List<AzureTableDataStoreSingleOperationException<TData>>()
                        {
                            new AzureTableDataStoreSingleOperationException<TData>("Failed to prepare entity for deletion: " + e.Message, e)
                            {
                                Entity = currentEntity
                            }
                        }
                    };
                }

                var ex = new AzureTableDataStoreBatchedOperationException<TData>(
                    "Failed to group entities into batches: " + e.Message, e);
                ex.BatchExceptionContexts.Add(new BatchExceptionContext<TData>()
                {
                    BatchEntities = entities.ToList()
                });
                throw ex;
            }


            // Run the prepared batches/items.


            if (batchingMode == BatchingMode.None)
            {
                var failedOps = new ConcurrentBag<AzureTableDataStoreSingleOperationException<TData>>();
                try
                {
                    var parallelOpGroups = ArrayExtensions.SplitToBatches(allEntities, ParallelTableOperationLimit);
                    foreach (var parallelOpGroup in parallelOpGroups)
                    {
                        var parallelOpsAsTasks = parallelOpGroup.Select(
                            item => RunAsSingleOperation(true, TableOperationType.Delete,
                                LargeBlobNullBehavior.DeleteBlob, item, failedOps));
                        var parallelTaskRuns = Task.WhenAll(parallelOpsAsTasks);
                        await parallelTaskRuns;
                    }

                    if (failedOps.Count > 1)
                    {
                        var exception = new AzureTableDataStoreMultiOperationException<TData>(
                            "One or more operations had errors");
                        exception.SingleOperationExceptions.AddRange(failedOps);
                        throw exception;
                    }

                    if (failedOps.Count == 1 && allEntities.Count == 1)
                    {
                        failedOps.TryTake(out var ex);
                        throw ex;
                    }
                }
                catch (AzureTableDataStoreSingleOperationException<TData>)
                {
                    throw;
                }
                catch (AzureTableDataStoreMultiOperationException<TData>)
                {
                    throw;
                }
                catch (Exception e) when (allEntities.Count > 1)
                {
                    var ex = new AzureTableDataStoreMultiOperationException<TData>(
                        $"Unexpected exception in multi-operation execution, see inner exception: " + e.Message, e);
                    ex.SingleOperationExceptions.AddRange(failedOps);
                    throw ex;
                }
                catch (Exception e) when (allEntities.Count == 1)
                {
                    var ex = new AzureTableDataStoreSingleOperationException<TData>(
                        $"Unexpected exception in single operation execution, see inner exception: " + e.Message, e);
                    ex.Entity = allEntities[0].SourceEntity;
                    throw ex;
                }

                return;
            }

            try
            {

                var failedTableBatches = new ConcurrentBag<BatchExceptionContext<TData>>();

                var parallelBatchGroups = ArrayExtensions.SplitToBatches(entityBatches, ParallelTableOperationLimit);
                foreach (var batchGroup in parallelBatchGroups)
                {
                    var batchGroupAsTasks = batchGroup.Select(
                        batchItems => RunAsBatchOperations(true, batchingMode, TableOperationType.Delete,
                            LargeBlobNullBehavior.DeleteBlob, batchItems, failedTableBatches));
                    var parallelTaskRuns = Task.WhenAll(batchGroupAsTasks);
                    await parallelTaskRuns;

                }

                // if any failed table batches or blob uploads, throw a collective AzureTableDataStoreBatchedOperationException
                if (failedTableBatches.Any())
                {
                    string exceptionMessage = "One or more Table delete batch calls failed, see BatchExceptionContexts for more information.";
                    throw new AzureTableDataStoreBatchedOperationException<TData>(exceptionMessage)
                    {
                        BatchExceptionContexts = failedTableBatches.ToList()
                    };
                }

            }
            catch (AzureTableDataStoreBatchedOperationException<TData>)
            {
                throw;
            }
            catch (Exception e)
            {
                var ex = new AzureTableDataStoreBatchedOperationException<TData>(
                    $"Unexpected exception in batch delete: " + e.Message,
                    e);
                ex.BatchExceptionContexts.Add(new BatchExceptionContext<TData>()
                {
                    BatchEntities = entities.ToList()
                });
                throw ex;
            }


        }

        private async Task DeleteInternalAsync(BatchingMode batchingMode, IList<EntityKeyPair> entityKeys)
        {
            var entities = entityKeys.Select(CreateEntityFromKeys)
                .ToArray();

            await DeleteInternalAsync(batchingMode, entities);
        }


        public async Task InsertOrReplaceAsync(BatchingMode batchingMode, params TData[] entities)
        {
            if (entities == null || entities.Length == 0)
                return;

            await InsertInternalAsync(entities, batchingMode, true);
        }

        private List<string> ValidateSelectExpressionAndExtractMembers(Expression expression)
        {
            List<string> mergedPropertyNames;
            try
            {
                mergedPropertyNames = AzureStorageQuerySelectTranslator.TranslateExpressionToMemberNames(
                    expression, EntityPropertyConverterOptions);

                if (mergedPropertyNames.Count == 0)
                {
                    throw new AzureTableDataStoreInternalException(
                        "Failed to resolve any properties from select expression");
                }
            }
            catch (AzureTableDataStoreInternalException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreInternalException("Failed to convert expression to member name selection: " + e.Message, e);
            }

            // Automatically warn if this happens: the user might accidentally attempt to change values that are keys, and we should let them know.
            if (mergedPropertyNames.Contains(_configuration.RowKeyProperty) ||
                mergedPropertyNames.Contains(_configuration.PartitionKeyProperty))
                throw new AzureTableDataStoreInternalException(
                    "Do not select the properties for PartitionKey or RowKey in a merge operation; " +
                    "they are immutable, and will automatically be used as the key to update the entity.");

            return mergedPropertyNames;
        }

        public async Task MergeAsync(BatchingMode batchingMode, Expression<Func<TData, object>> selectMergedPropertiesExpression, 
            LargeBlobNullBehavior largeBlobNullBehavior = LargeBlobNullBehavior.IgnoreProperty, params TData[] entities)
        {
            if (entities == null || entities.Length == 0)
                return;

            await MergeInternalAsync(selectMergedPropertiesExpression, batchingMode, largeBlobNullBehavior, entities.Select(x => new DataStoreEntity<TData>("*", x)).ToArray());
        }
        
        private async Task MergeInternalAsync(Expression<Func<TData, object>> selectMergedPropertiesExpression, BatchingMode batchingMode, LargeBlobNullBehavior largeBlobNullBehavior, DataStoreEntity<TData>[] entities)
        {

            // Run some pre-merge checks and throw early on errors.

            var propertyNames = ValidateSelectExpressionAndExtractMembers(selectMergedPropertiesExpression);

            ValidateBatchParameters(entities.Select(x => x.Value).ToArray(), batchingMode, false, largeBlobNullBehavior);

            var entityPartitionGroups = entities.GroupBy(x => _entityTypePartitionKeyPropertyInfo.GetValue(x.Value))
                .ToDictionary(x => (string)x.Key, x => x.ToList());

            var entityBatches = new List<List<BatchItem>>();
            var validationException = new AzureTableDataStoreEntityValidationException<TData>("Client side validation failed for some entities. "
                + $"See {nameof(AzureTableDataStoreEntityValidationException<TData>.EntityValidationErrors)} for details.");


            // Collect entities into batches for merge.
            // Throw if we get errors.

            DataStoreEntity<TData> currentEntity = default;
            var allEntities = new List<BatchItem>();

            try
            {

                foreach (var group in entityPartitionGroups)
                {
                    List<BatchItem> entityBatch = null;

                    foreach (var entity in group.Value)
                    {
                        currentEntity = entity;
                        var entityKeys = GetEntityKeys(entity.Value);
                        var entityData = ExtractEntityProperties(entity.Value, true);

                        var selectedPropertyValues = new Dictionary<string, EntityProperty>();

                        entityData.CollectionPropertyRefs.ForEach(@ref =>
                            entityData.PropertyDictionary.Add(@ref.FlattenedPropertyName,
                                EntityProperty.GeneratePropertyForString(
                                    JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                        foreach (var @ref in entityData.BlobPropertyRefs)
                        {
                            if (@ref.StoredInstance == null)
                                continue;

                            var stream = await @ref.StoredInstance.AsyncDataStream.Value;
                            @ref.StoredInstance.Length = stream.Length;
                            entityData.PropertyDictionary.Add(@ref.FlattenedPropertyName,
                                EntityProperty.GeneratePropertyForString(
                                    JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings)));
                        }

                        foreach (var propertyName in propertyNames)
                        {
                            if (entityData.PropertyDictionary.ContainsKey(propertyName))
                            {
                                var property = entityData.PropertyDictionary[propertyName];
                                selectedPropertyValues.Add(propertyName, property);
                            }
                        }

                        var tableEntity = new DynamicTableEntity()
                        {
                            PartitionKey = entityKeys.partitionKey,
                            RowKey = entityKeys.rowKey,
                            ETag = entity.ETag,
                            Properties = selectedPropertyValues
                        };

                        if (UseClientSideValidation)
                        {
                            var entityValidationErrors = SerializationUtils.ValidateProperties(tableEntity);
                            var blobPaths = entityData.BlobPropertyRefs.Select(x =>
                                BuildBlobPath(x, entityKeys.partitionKey, entityKeys.rowKey));
                            var pathValidations = blobPaths.Select(x => SerializationUtils.ValidateBlobPath(x));
                            entityValidationErrors.AddRange(pathValidations.Where(x => x.Count > 0).SelectMany(x => x));

                            if (entityValidationErrors.Any())
                            {
                                validationException.EntityValidationErrors.Add(entity.Value, entityValidationErrors);
                            }
                        }

                        if (batchingMode == BatchingMode.None)
                        {
                            allEntities.Add(new BatchItem()
                            {
                                SourceEntity = entity.Value,
                                SerializedEntity = tableEntity,
                                LargeBlobRefs = entityData.BlobPropertyRefs
                            });
                            continue;
                        }


                        if (entityBatch == null)
                        {
                            entityBatch = new List<BatchItem>();
                            entityBatches.Add(entityBatch);
                        }

                        var batchSize = SerializationUtils.CalculateApproximateBatchRequestSize(
                            entityBatch.Select(x => x.SerializedEntity).Append(tableEntity).ToArray());

                        var entitySizeInBytes = SerializationUtils.CalculateApproximateBatchEntitySize(tableEntity);
                        if (batchSize < MaxSingleBatchSize && entityBatch.Count < MaxSingleBatchCount)
                        {
                            entityBatch.Add(new BatchItem()
                            {
                                SourceEntity = entity.Value,
                                SerializedEntity = tableEntity,
                                LargeBlobRefs = entityData.BlobPropertyRefs
                            });
                        }
                        else
                        {
                            // Strict mode means a single batch transaction, so we'll just throw if it doesn't fit into one batch.
                            if (batchingMode == BatchingMode.Strict)
                            {
                                throw new AzureTableDataStoreInternalException("Entities do not fit into a single batch, unable to insert with BatchingMode.Strict");
                            }
                            entityBatch = new List<BatchItem>
                            {
                                new BatchItem()
                                {
                                    SourceEntity = entity.Value,
                                    SerializedEntity = tableEntity,
                                    LargeBlobRefs = entityData.BlobPropertyRefs
                                }
                            };
                            entityBatches.Add(entityBatch);
                        }
                    }
                }

                if (validationException.EntityValidationErrors.Any())
                    throw validationException;
            }
            catch (AzureTableDataStoreEntityValidationException<TData>)
            {
                throw;
            }
            catch (SerializationException e)
            {
                if (batchingMode == BatchingMode.None && entities.Length < 2)
                {
                    throw new AzureTableDataStoreSingleOperationException<TData>(
                        "Serialization of the data failed: " + e.Message, e)
                    {
                        Entity = entities[0].Value
                    };
                }

                if (batchingMode == BatchingMode.None)
                {
                    throw new AzureTableDataStoreMultiOperationException<TData>(
                        "Serialization of the data failed: " + e.Message, e)
                    {
                        SingleOperationExceptions = new List<AzureTableDataStoreSingleOperationException<TData>>()
                        {
                            new AzureTableDataStoreSingleOperationException<TData>("Serialization error: " + e.Message, e)
                            {
                                Entity = currentEntity.Value
                            }
                        }
                    };
                }

                var ex = new AzureTableDataStoreBatchedOperationException<TData>(
                    "Serialization of the data failed: " + e.Message, e);
                ex.BatchExceptionContexts.Add(new BatchExceptionContext<TData>()
                {
                    BatchEntities = entities.Select(x => x.Value).ToList(),
                    CurrentEntity = currentEntity != null ? currentEntity.Value : default
                });
                throw ex;
            }
            catch (Exception e)
            {
                if (batchingMode == BatchingMode.None && entities.Length < 2)
                {
                    throw new AzureTableDataStoreSingleOperationException<TData>(
                        "Failed to prepare the entity for merge: " + e.Message, e)
                    {
                        Entity = entities[0].Value
                    };
                }

                if (batchingMode == BatchingMode.None)
                {
                    throw new AzureTableDataStoreMultiOperationException<TData>(
                        "Failed to prepare the entities for merge: " + e.Message, e)
                    {
                        SingleOperationExceptions = new List<AzureTableDataStoreSingleOperationException<TData>>()
                        {
                            new AzureTableDataStoreSingleOperationException<TData>("Failed to prepare entity for insert/replace: " + e.Message, e)
                            {
                                Entity = currentEntity != null ? currentEntity.Value : default
                            }
                        }
                    };
                }

                var ex = new AzureTableDataStoreBatchedOperationException<TData>(
                    "Failed to group entities into batches: " + e.Message, e);
                ex.BatchExceptionContexts.Add(new BatchExceptionContext<TData>()
                {
                    BatchEntities = entities.Select(x => x.Value).ToList()
                });
                throw ex;
            }


            // Run the prepared batches/items.


            if (batchingMode == BatchingMode.None)
            {
                var failedOps = new ConcurrentBag<AzureTableDataStoreSingleOperationException<TData>>();
                try
                {
                    var parallelOpGroups = ArrayExtensions.SplitToBatches(allEntities, 20);
                    foreach (var parallelOpGroup in parallelOpGroups)
                    {
                        var parallelOpsAsTasks = parallelOpGroup.Select(
                            item => RunAsSingleOperation(true, TableOperationType.Merge,
                                largeBlobNullBehavior, item, failedOps));
                        var parallelTaskRuns = Task.WhenAll(parallelOpsAsTasks);
                        await parallelTaskRuns;
                    }

                    if (failedOps.Count > 1)
                    {
                        var exception = new AzureTableDataStoreMultiOperationException<TData>(
                            "One or more operations had errors");
                        exception.SingleOperationExceptions.AddRange(failedOps);
                        throw exception;
                    }

                    if (failedOps.Count == 1 && allEntities.Count == 1)
                    {
                        failedOps.TryTake(out var ex);
                        throw ex;
                    }
                }
                catch (AzureTableDataStoreSingleOperationException<TData>)
                {
                    throw;
                }
                catch (AzureTableDataStoreMultiOperationException<TData>)
                {
                    throw;
                }
                catch (Exception e) when (allEntities.Count > 1)
                {
                    var ex = new AzureTableDataStoreMultiOperationException<TData>(
                        $"Unexpected exception in multi-operation execution, see inner exception: " + e.Message, e);
                    ex.SingleOperationExceptions.AddRange(failedOps);
                    throw ex;
                }
                catch (Exception e) when (allEntities.Count == 1)
                {
                    var ex = new AzureTableDataStoreSingleOperationException<TData>(
                        $"Unexpected exception in single operation execution, see inner exception: " + e.Message, e);
                    ex.Entity = allEntities[0].SourceEntity;
                    throw ex;
                }

                return;
            }

            try
            {

                var failedTableBatches = new ConcurrentBag<BatchExceptionContext<TData>>();

                var parallelBatchGroups = ArrayExtensions.SplitToBatches(entityBatches, ParallelTableOperationLimit);
                foreach (var batchGroup in parallelBatchGroups)
                {
                    var batchGroupAsTasks = batchGroup.Select(
                        batchItems => RunAsBatchOperations(true, batchingMode, TableOperationType.Merge,
                            largeBlobNullBehavior, batchItems, failedTableBatches));
                    var parallelTaskRuns = Task.WhenAll(batchGroupAsTasks);
                    await parallelTaskRuns;

                }

                // if any failed table batches or blob uploads, throw a collective AzureTableDataStoreBatchedOperationException
                if (failedTableBatches.Any())
                {
                    string exceptionMessage = "One or more Table merge batch calls failed, see BatchExceptionContexts for more information.";
                    throw new AzureTableDataStoreBatchedOperationException<TData>(exceptionMessage)
                    {
                        BatchExceptionContexts = failedTableBatches.ToList()
                    };
                }

            }
            catch (AzureTableDataStoreBatchedOperationException<TData>)
            {
                throw;
            }
            catch (Exception e)
            {
                var ex = new AzureTableDataStoreBatchedOperationException<TData>(
                    $"Unexpected exception in batch merge: " + e.Message,
                    e);
                ex.BatchExceptionContexts.Add(new BatchExceptionContext<TData>()
                {
                    BatchEntities = entities.Select(x => x.Value).ToList()
                });
                throw ex;
            }


        }


        public async Task MergeAsync(BatchingMode batchingMode, Expression<Func<TData, object>> selectMergedPropertiesExpression, 
            LargeBlobNullBehavior largeBlobNullBehavior = LargeBlobNullBehavior.IgnoreProperty, params DataStoreEntity<TData>[] entities)
        {
            if (entities == null || entities.Length == 0)
                return;

            await MergeInternalAsync(selectMergedPropertiesExpression, batchingMode, largeBlobNullBehavior, entities);
        }

        /// <summary>
        /// Extracts entity properties into flattened dictionary, blob property references and collection
        /// property references.
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        private (
            Dictionary<string, EntityProperty> PropertyDictionary,
            List<ReflectionUtils.PropertyRef<LargeBlob>> BlobPropertyRefs,
            List<ReflectionUtils.PropertyRef<ICollection>> CollectionPropertyRefs
            ) ExtractEntityProperties(TData entity, bool includeNullBlobs = false)
        {
            var blobPropertyRefs =
                ReflectionUtils.GatherPropertiesWithBlobsRecursive(entity, EntityPropertyConverterOptions, includeNulls: includeNullBlobs);
            var collectionPropertyRefs =
                ReflectionUtils.GatherPropertiesWithCollectionsRecursive(entity, EntityPropertyConverterOptions);
            var allSpecialPropertyRefs = blobPropertyRefs.Cast<ReflectionUtils.PropertyRef>()
                .Concat(collectionPropertyRefs).ToList();
            StripSpeciallyHandledProperties(allSpecialPropertyRefs);
            var propertyDictionary = EntityPropertyConverter.Flatten(entity, EntityPropertyConverterOptions, null);
            RestoreSpeciallyHandledProperties(allSpecialPropertyRefs);

            propertyDictionary.Remove(_configuration.RowKeyProperty);
            propertyDictionary.Remove(_configuration.PartitionKeyProperty);

            return (propertyDictionary, blobPropertyRefs, collectionPropertyRefs);
        }

        public async Task<IList<TData>> ListAsync(Expression<Func<TData, object>> selectExpression = null, int? limit = null)
        {
            var result = await FindWithMetadataAsyncInternal(null, selectExpression, limit);
            return result.Select(x => x.Value).ToList();
        }

        public async Task<IList<DataStoreEntity<TData>>> ListWithMetadataAsync(
            Expression<Func<TData, object>> selectExpression = null, int? limit = null)
        {
            var result = await FindWithMetadataAsyncInternal(null, selectExpression, limit);
            return result;
        }

        public async Task<TData> GetAsync(Expression<Func<TData, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null)
        {
            var result = await FindWithMetadataAsyncInternal(queryExpression, selectExpression, 1);
            var first = result.FirstOrDefault();
            return first != null ? first.Value : default(TData);

            //return await GetAsyncInternal(queryExpression);
        }


        public async Task<TData> GetAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null)
        {
            var result = await FindWithMetadataAsyncInternal(queryExpression, selectExpression, 1);
            var first = result.FirstOrDefault();
            return first != null ? first.Value : default(TData);

            // return await GetAsyncInternal(queryExpression);
        }

        private async Task<List<DataStoreEntity<TData>>> FindWithMetadataAsyncInternal(Expression queryExpression, Expression<Func<TData, object>> selectExpression = null, int? limitCount = null)
        {
            try
            {
                string filterString = null;
                try
                {
                    if(queryExpression != null)
                        filterString = AzureStorageQueryTranslator.TranslateExpression(queryExpression,
                            _configuration.PartitionKeyProperty, _configuration.RowKeyProperty, EntityPropertyConverterOptions);
                }
                catch (Exception e)
                {
                    throw new AzureTableDataStoreInternalException(
                        "Failed to translate query expression into a query: " + e.Message, e);
                }


                List<string> selectedProperties = null;
                if (selectExpression != null)
                {
                    try
                    {
                        selectedProperties = AzureStorageQuerySelectTranslator.TranslateExpressionToMemberNames(
                            selectExpression,
                            EntityPropertyConverterOptions);
                        selectedProperties = selectedProperties.Select(x =>
                            x == _configuration.RowKeyProperty ? "RowKey" :
                            x == _configuration.PartitionKeyProperty ? "PartitionKey" : x
                        ).ToList();
                    }
                    catch (Exception e)
                    {
                        throw new AzureTableDataStoreInternalException(
                            "Failed to translate select expression to property names: " + e.Message, e);
                    }

                }

                var tableRef = GetTable();
                var query = new TableQuery { FilterString = filterString, TakeCount = limitCount != null && limitCount <= 1000 ? limitCount : 1000 };
                if (selectedProperties != null)
                    query.SelectColumns = selectedProperties;

                try
                {
                    TableContinuationToken token = null;
                    var foundEntities = new List<DataStoreEntity<TData>>();

                    do
                    {
                        var results = await tableRef.ExecuteQuerySegmentedAsync(query, TransformQueryResult, token);
                        token = results.ContinuationToken;
                        foundEntities.AddRange(results.Results);
                        if (limitCount != null && foundEntities.Count >= limitCount)
                            break;
                    } while (token != null);

                    return limitCount != null ? foundEntities.Take(limitCount.Value).ToList() : foundEntities;

                }
                catch (Exception e)
                {
                    throw new AzureTableDataStoreQueryException($"Failed to retrieve entities with query '{queryExpression}': " + e.Message, e);
                }
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreQueryException(
                    $"Failed to retrieve entities with query '{queryExpression}': " + e.Message, e);
            }
        }

        public async Task<DataStoreEntity<TData>> GetWithMetadataAsync(Expression<Func<TData, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null)
        {
            var result = await FindWithMetadataAsyncInternal(queryExpression, selectExpression, 1);
            var first = result.FirstOrDefault();
            return first;
        }

        public async Task<DataStoreEntity<TData>> GetWithMetadataAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null)
        {
            var result = await FindWithMetadataAsyncInternal(queryExpression, selectExpression, 1);
            var first = result.FirstOrDefault();
            return first;
        }


        public async Task<IList<DataStoreEntity<TData>>> FindWithMetadataAsync(Expression<Func<TData, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null, int? limit = null)
        {
            return await FindWithMetadataAsyncInternal(queryExpression, selectExpression, limit);
        }

        public async Task<IList<DataStoreEntity<TData>>> FindWithMetadataAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null, int? limit = null)
        {
            return await FindWithMetadataAsyncInternal(queryExpression, selectExpression, limit);
        }

        public async Task<IList<TData>> FindAsync(Expression<Func<TData, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null, int? limit = null)
        {
            var results = await FindWithMetadataAsyncInternal(queryExpression, selectExpression, limit);
            return results.Select(x => x.Value).ToList();
        }

        public async Task<IList<TData>> FindAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null, int? limit = null)
        {
            var results = await FindWithMetadataAsyncInternal(queryExpression, selectExpression, limit);
            return results.Select(x => x.Value).ToList();
        }

        public async Task DeleteTableAndBlobContainerAsync()
        {
            var exceptions = new List<Exception>();
            try
            {
                await GetTable().DeleteIfExistsAsync();
            }
            catch (Exception e)
            {
                exceptions.Add(new AzureTableDataStoreInternalException("Failed to delete table: " + e.Message, e));
            }

            try
            {
                await GetContainerClient().DeleteIfExistsAsync();
            }
            catch (Exception e)
            {
                exceptions.Add(new AzureTableDataStoreInternalException("Failed to delete blob container: " + e.Message, e));
            }

            if(exceptions.Any())
                throw new AzureTableDataStoreInternalException("Failed to delete table and/or blob container", 
                    exceptions.Count == 1 ? exceptions.First() : new AggregateException(exceptions));
        }

        public async Task<long> CountRowsAsync(Expression<Func<TData, bool>> queryExpression)
        {
            try
            {
                long resultCount = 0;
                string filterString;
                try
                {
                    filterString = AzureStorageQueryTranslator.TranslateExpression(queryExpression,
                        _configuration.PartitionKeyProperty, _configuration.RowKeyProperty,
                        EntityPropertyConverterOptions);
                }
                catch (Exception e)
                {
                    throw new AzureTableDataStoreInternalException(
                        "Failed to translate query expression into a query: " + e.Message, e);
                }


                var tableRef = GetTable();
                var query = new TableQuery()
                {
                    FilterString = filterString,
                    SelectColumns = new List<string>()
                    {
                        "PartitionKey",
                        "RowKey"
                    }
                };


                TableContinuationToken continuationToken = null;
                do
                {
                    var segment = await tableRef.ExecuteQuerySegmentedAsync(query, continuationToken);
                    resultCount += segment.Results.Count;
                    continuationToken = segment.ContinuationToken;
                } while (continuationToken != null);

                return resultCount;

            }
            catch (AzureTableDataStoreInternalException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreInternalException("Row count attempt threw an exception: " + e.Message, e);
            }
        }

        public async Task<long> CountRowsAsync()
        {
            long resultCount = 0;
            try
            {
                var tableRef = GetTable();
                var tableQuery = new TableQuery()
                {
                    SelectColumns = new List<string>()
                    {
                        "PartitionKey",
                        "RowKey"
                    },
                    TakeCount = 1000
                };

                TableContinuationToken continuationToken = null;
                do
                {
                    var segment = await tableRef.ExecuteQuerySegmentedAsync(tableQuery, continuationToken);
                    resultCount += segment.Results.Count;
                    continuationToken = segment.ContinuationToken;
                } while (continuationToken != null);

                return resultCount;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreInternalException("Row count attempt threw an exception: " + e.Message, e);
            }
        }

        public async Task EnumerateWithMetadataAsync(Expression<Func<TData, bool>> queryExpression, int entitiesPerPage,
            EnumeratorFunc<DataStoreEntity<TData>> enumeratorFunc, TableContinuationToken continuationToken = null)
        {
            await EnumerateWithMetadataInternalAsync(queryExpression, entitiesPerPage, enumeratorFunc,
                continuationToken);
        }

        public async Task EnumerateWithMetadataAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression,
            int entitiesPerPage, EnumeratorFunc<DataStoreEntity<TData>> enumeratorFunc,
            TableContinuationToken continuationToken = null)
        {
            await EnumerateWithMetadataInternalAsync(queryExpression, entitiesPerPage, enumeratorFunc,
                continuationToken);
        }

        private async Task EnumerateWithMetadataInternalAsync(Expression queryExpression, int entitiesPerPage,
            EnumeratorFunc<DataStoreEntity<TData>> enumeratorFunc, TableContinuationToken continuationToken = null)
        {

            if (entitiesPerPage < 1 || entitiesPerPage > 1000)
                throw new AzureTableDataStoreInternalException($"Parameter {nameof(entitiesPerPage)} must have value between 1 and 1000");

            if (enumeratorFunc == null)
                throw new AzureTableDataStoreInternalException($"Parameter {nameof(enumeratorFunc)} cannot be null");

            try
            {
                string filterString;
                try
                {
                    filterString = AzureStorageQueryTranslator.TranslateExpression(queryExpression,
                        _configuration.PartitionKeyProperty, _configuration.RowKeyProperty, EntityPropertyConverterOptions);
                }
                catch (Exception e)
                {
                    throw new AzureTableDataStoreInternalException(
                        "Failed to translate query expression into a query: " + e.Message, e);
                }

                var tableRef = GetTable();
                var query = new TableQuery { FilterString = filterString, TakeCount = entitiesPerPage };


                try
                {
                    TableContinuationToken token = continuationToken;

                    do
                    {
                        var results = await tableRef.ExecuteQuerySegmentedAsync(query, TransformQueryResult, token);
                        token = results.ContinuationToken;
                        bool continueEnumeration = false;
                        try
                        {
                            continueEnumeration = await enumeratorFunc(results.Results, token);
                        }
                        catch (Exception e)
                        {
                            throw new AzureTableDataStoreInternalException(
                                "Enumerator function threw an exception: " + e.Message, e);
                        }

                        if (!continueEnumeration)
                            break;
                    } while (token != null);

                }
                catch (AzureTableDataStoreInternalException)
                {
                    throw;
                }
                catch (Exception e)
                {
                    throw new AzureTableDataStoreQueryException($"Failed to retrieve entities with query '{queryExpression}': " + e.Message, e);
                }
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreQueryException(
                    $"Failed to enumerate entities with query '{queryExpression}': " + e.Message, e);
            }
        }


        private DataStoreEntity<TData> TransformQueryResult(string partitionKey,
            string rowKey,
            DateTimeOffset timestamp,
            IDictionary<string, EntityProperty> properties,
            string etag)
        {
            if(!properties.ContainsKey(_configuration.RowKeyProperty))
                properties.Add(_configuration.RowKeyProperty, EntityProperty.CreateEntityPropertyFromObject(rowKey));
            if(!properties.ContainsKey(_configuration.PartitionKeyProperty))
                properties.Add(_configuration.PartitionKeyProperty, EntityProperty.CreateEntityPropertyFromObject(partitionKey));

            var blobRefProperties = _dataTypeLargeBlobRefs;
            var collRefProperties = _dataTypeCollectionRefs;

            var blobRefPropertyValues = new Dictionary<string, string>();
            var collRefPropertyValues = new Dictionary<string, string>();

            var foundBlobRefs = blobRefProperties.Where(x => properties.ContainsKey(x.FlattenedPropertyName));
            foreach (var @ref in foundBlobRefs)
            {
                blobRefPropertyValues.Add(@ref.FlattenedPropertyName,
                    properties[@ref.FlattenedPropertyName].StringValue);
                properties.Remove(@ref.FlattenedPropertyName);
            }

            var foundCollRefs = collRefProperties.Where(x => properties.ContainsKey(x.FlattenedPropertyName));
            foreach (var @ref in foundCollRefs)
            {
                collRefPropertyValues.Add(@ref.FlattenedPropertyName,
                    properties[@ref.FlattenedPropertyName].StringValue);
                properties.Remove(@ref.FlattenedPropertyName);
            }

            var converted = EntityPropertyConverter.ConvertBack<TData>(properties, EntityPropertyConverterOptions, null);

            var convertedObjectBlobPropRefs = ReflectionUtils.GatherPropertiesWithBlobsRecursive(converted, EntityPropertyConverterOptions,
                includeNulls: true);
            var convertedObjectCollPropRefs = ReflectionUtils.GatherPropertiesWithCollectionsRecursive(converted, EntityPropertyConverterOptions,
                includeNulls: true);

            foreach (var value in blobRefPropertyValues)
            {
                var flattenedPropName = value.Key;
                var propValue = value.Value;

                var propInfo = convertedObjectBlobPropRefs.First(x => x.FlattenedPropertyName == flattenedPropName);
                var deserializedValue = JsonConvert.DeserializeObject<LargeBlob>(propValue, _jsonSerializerSettings);

                if (propInfo.SourceObject == null)
                    propInfo.SourceObject = CreateObjectHierarchyForProperty(converted, flattenedPropName);

                propInfo.Property.SetValue(propInfo.SourceObject, deserializedValue);
                if (deserializedValue != null)
                {
                    var filename = deserializedValue.Filename; 
                    var blobPath = BuildBlobPath(_configuration.StorageTableName, partitionKey, rowKey, flattenedPropName, filename);
                    deserializedValue.AsyncDataStream = new Lazy<Task<Stream>>(() => GetBlobStreamFromAzureBlobStorage(converted, deserializedValue, blobPath));
                    deserializedValue.BlobClient = _blobServiceClient.GetBlobContainerClient(_configuration.BlobContainerName).GetBlobClient(blobPath);
                    deserializedValue.BlobUriBuilder = _blobUriBuilder;
                }
            }

            foreach (var value in collRefPropertyValues)
            {
                var flattenedPropName = value.Key;
                var propValue = value.Value;

                var propInfo = convertedObjectCollPropRefs.First(x => x.FlattenedPropertyName == flattenedPropName);
                var deserializedValue = JsonConvert.DeserializeObject(propValue, propInfo.Property.PropertyType, _jsonSerializerSettings);

                if (propInfo.SourceObject == null)
                    propInfo.SourceObject = CreateObjectHierarchyForProperty(converted, flattenedPropName);

                propInfo.Property.SetValue(propInfo.SourceObject, deserializedValue);
            }

            return new DataStoreEntity<TData>(etag, converted, timestamp);

        }

        private object CreateObjectHierarchyForProperty(object rootObject, string flattenedPropName)
        {
            var propertyPath = flattenedPropName.Split(
                new string[] { EntityPropertyConverterOptions.PropertyNameDelimiter }, StringSplitOptions.None);

            object current = rootObject;
            for (var i = 0; i < propertyPath.Length - 1; i++)
            {
                var property = current.GetType().GetProperty(propertyPath[i], BindingFlags.Instance | BindingFlags.Public);
                var propertyValue = property.GetValue(current);
                if (propertyValue == null)
                {
                    propertyValue = Activator.CreateInstance(property.PropertyType);
                    property.SetValue(current, propertyValue);
                }

                current = propertyValue;
            }

            return current;
        }

    }
}