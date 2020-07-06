using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;

[assembly: InternalsVisibleTo("AzureTableDataStore.Tests")]

namespace AzureTableDataStore
{
    /// <inheritdoc cref="ITableDataStore"/>
    public class TableDataStore<TData> : ITableDataStore<TData> where TData:new()
    {
        private class Configuration
        {
            public string BlobContainerName { get; set; }
            public PublicAccessType BlobContainerAccessType { get; set; }
            public string StorageTableName { get; set; }
            public string PartitionKeyProperty { get; set; }
            public string RowKeyProperty { get; set; }
        }

        private readonly object _syncLock = new object();
        public string Name { get; private set; }
        private CloudStorageAccount _cloudStorageAccount;
        private BlobServiceClient _blobServiceClient;
        private JsonSerializerSettings _jsonSerializerSettings = new JsonSerializerSettings();

        private Configuration _configuration;
        private bool _containerClientInitialized = false;
        private bool _tableClientInitialized = false;

        private PropertyInfo _entityTypeRowKeyPropertyInfo;
        private PropertyInfo _entityTypePartitionKeyPropertyInfo;

        public EntityPropertyConverterOptions EntityPropertyConverterOptions { get; set; } = new EntityPropertyConverterOptions();


        public TableDataStore(string tableStorageConnectionString, string tableName, string blobContainerName, PublicAccessType blobContainerAccessType,
            string blobStorageConnectionString = null, string storeName = null, string partitionKeyProperty = null, string rowKeyProperty = null)
        {
            Name = storeName ?? "default";
            
            
            _cloudStorageAccount = CloudStorageAccount.Parse(tableStorageConnectionString);
            _blobServiceClient = new BlobServiceClient(blobStorageConnectionString ?? tableStorageConnectionString);
            _configuration = new Configuration()
            {
                BlobContainerAccessType = blobContainerAccessType,
                BlobContainerName = blobContainerName,
                StorageTableName = tableName,
                PartitionKeyProperty = ResolvePartitionKeyProperty(partitionKeyProperty),
                RowKeyProperty = ResolveRowKeyProperty(rowKeyProperty)
            };
            PostConfigure();
        }

        public TableDataStore(StorageCredentials tableStorageCredentials, StorageUri tableStorageUri, string tableName,
            StorageSharedKeyCredential blobStorageCredentials, Uri blobStorageServiceUri, string blobContainerName, PublicAccessType blobContainerAccessType,
            string storeName = null, string partitionKeyProperty = null, string rowKeyProperty = null)
        {
            Name = storeName ?? "default";
            _cloudStorageAccount = new CloudStorageAccount(tableStorageCredentials, tableStorageUri);
            _blobServiceClient = new BlobServiceClient(blobStorageServiceUri, blobStorageCredentials);
            _configuration = new Configuration()
            {
                BlobContainerAccessType = blobContainerAccessType,
                BlobContainerName = blobContainerName,
                StorageTableName = tableName,
                PartitionKeyProperty = ResolvePartitionKeyProperty(partitionKeyProperty),
                RowKeyProperty = ResolveRowKeyProperty(rowKeyProperty)
            };
            PostConfigure();
        }

        private void PostConfigure()
        {
            _entityTypeRowKeyPropertyInfo = typeof(TData).GetProperty(_configuration.RowKeyProperty);
            _entityTypePartitionKeyPropertyInfo = typeof(TData).GetProperty(_configuration.PartitionKeyProperty);
        }

        private string ResolvePartitionKeyProperty(string inputPartitionKeyProperty)
        {
            var entryType = typeof(TData);
            var properties = entryType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            if (!string.IsNullOrEmpty(inputPartitionKeyProperty))
            {    
                if(properties.All(x => x.Name != inputPartitionKeyProperty))
                    throw new AzureTableDataStoreException($"Given partition key property name '{inputPartitionKeyProperty}' " +
                        $"is not a property in the data type '{entryType.Name}', please specify a valid property to act as partition key!",
                        AzureTableDataStoreException.ProblemSourceType.Configuration);

                return inputPartitionKeyProperty;
            }

            var partitionKeyProperty = properties.FirstOrDefault(x => x.GetCustomAttributes(typeof(TablePartitionKeyAttribute)).Any());
            if (partitionKeyProperty != null)
                return partitionKeyProperty.Name;

            throw new AzureTableDataStoreException($"Unable to resolve partition key for Type '{entryType.Name}', " +
                $"no explicit partition key was provided in {nameof(TableDataStore<TData>)} constructor and the Type has " +
                $"no property with the '{nameof(TablePartitionKeyAttribute)}' attribute.",
                AzureTableDataStoreException.ProblemSourceType.Configuration);
        }

        private string ResolveRowKeyProperty(string inputRowKeyProperty)
        {
            var entryType = typeof(TData);
            var properties = entryType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            if (!string.IsNullOrEmpty(inputRowKeyProperty))
            {
                if (properties.All(x => x.Name != inputRowKeyProperty))
                    throw new AzureTableDataStoreException($"Given row key property name '{inputRowKeyProperty}' " +
                        $"is not a property in the data type '{entryType.Name}', please specify a valid property to act as row key!");

                return inputRowKeyProperty;
            }

            var rowKeyProperty = properties.FirstOrDefault(x => x.GetCustomAttributes(typeof(TableRowKeyAttribute)).Any());
            if (rowKeyProperty != null)
                return rowKeyProperty.Name;

            throw new AzureTableDataStoreException($"Unable to resolve row key for Type '{entryType.Name}', " +
                $"no explicit row key was provided in {nameof(TableDataStore<TData>)} constructor and the Type has " +
                $"no property with the '{nameof(TableRowKeyAttribute)}' attribute.");
        }

        private (string partitionKey, string rowKey) GetEntityKeys(TData entry) =>
            (_entityTypePartitionKeyPropertyInfo.GetValue(entry).ToString(), _entityTypeRowKeyPropertyInfo.GetValue(entry).ToString());

        
        public async Task InsertAsync(bool useBatching, params TData[] entities)
        {
            switch (entities?.Length)
            {
                case 0:
                    return;
                case 1:
                    await InsertOneAsync(entities[0], false);
                    break;
                default:
                    if (useBatching) await InsertBatchedAsync(entities, false);
                    else await InsertMultipleAsync(entities, false);
                    return;
            }
        }

        private BlobContainerClient GetContainerClient()
        {
            lock (_syncLock)
            {
                if (!_containerClientInitialized)
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
                        throw new AzureTableDataStoreException("Unable to initialize blob container (CreateIfNotExists): " + e.Message,
                            AzureTableDataStoreException.ProblemSourceType.BlobStorage, e);
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
                try
                {
                    if (!_tableClientInitialized)
                    {
                        var cloudTableClient = _cloudStorageAccount.CreateCloudTableClient();
                        var tableRef = cloudTableClient.GetTableReference(_configuration.StorageTableName);
                        tableRef.CreateIfNotExists();
                        _tableClientInitialized = true;
                        return tableRef;
                    }
                }
                catch (Exception e)
                {
                    throw new AzureTableDataStoreException("Unable to initialize table (CreateIfNotExists): " + e.Message,
                        AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
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


        private async Task InsertOneAsync(TData entity, bool allowReplace)
        {
            try
            {
                var entityData = ExtractEntityProperties(entity);
                var entityKeys = GetEntityKeys(entity);
                var tableRef = GetTable();

                // Check if the row already exists if we have blobs to upload - we don't want to upload them
                // if the table insert itself may fail.
                if (entityData.BlobPropertyRefs.Any())
                {
                    TableOperation existsOp = TableOperation.Retrieve(entityKeys.partitionKey, entityKeys.rowKey, new List<string>());
                    try
                    {
                        await tableRef.ExecuteAsync(existsOp);
                        throw new AzureTableDataStoreException("Entity with partition key " +
                            $"'{entityKeys.partitionKey}', row key '{entityKeys.rowKey}' already exists, cannot insert.",
                            AzureTableDataStoreException.ProblemSourceType.TableStorage);
                    }
                    catch (StorageException e)
                    {
                        if (e.RequestInformation.HttpStatusCode != 404)
                        {
                            throw new AzureTableDataStoreException("Failed to check if entity exists with partition key " +
                                $"'{entityKeys.partitionKey}', row key '{entityKeys.rowKey}'",
                                AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
                        }
                    }
                }

                var uploadTasks = entityData.BlobPropertyRefs
                    .Select(x => UploadBlobAndUpdateReference(x, entityKeys.partitionKey, entityKeys.rowKey)).ToArray();
                await Task.WhenAll(uploadTasks);

                entityData.CollectionPropertyRefs.ForEach(@ref =>
                    entityData.PropertyDictionary.Add(@ref.FlattenedPropertyName, EntityProperty.GeneratePropertyForString(
                        JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                entityData.BlobPropertyRefs.ForEach(@ref =>
                    entityData.PropertyDictionary.Add(@ref.FlattenedPropertyName, EntityProperty.GeneratePropertyForString(
                        JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                var tableEntity = new DynamicTableEntity(entityKeys.partitionKey, entityKeys.rowKey, "*",
                    entityData.PropertyDictionary);

                var insertOp = TableOperation.Insert(tableEntity);
                if(allowReplace)
                    insertOp = TableOperation.InsertOrReplace(tableEntity);

                await tableRef.ExecuteAsync(insertOp);

            }
            catch (AzureTableDataStoreException)
            {
                throw;
            }
            catch (SerializationException e)
            {
                throw new AzureTableDataStoreException("Serialization of the data failed: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.Data, e);
            }
            catch (Exception e)
            {
                if (e.GetType().Namespace.StartsWith("Microsoft.Azure.Cosmos"))
                    throw new AzureTableDataStoreException("Insert operation failed, outlying Table Storage threw an exception: " + e.Message,
                        AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
                if (e.GetType().Namespace.StartsWith("Azure.Storage") || e is Azure.RequestFailedException)
                    throw new AzureTableDataStoreException("Insert operation failed and entity was not inserted, outlying Blob Storage threw an exception: " + e.Message,
                        AzureTableDataStoreException.ProblemSourceType.BlobStorage, e);
                throw new AzureTableDataStoreException("Insert operation failed, unhandlable exception: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.General, e);
            }
        }



        private async Task UploadBlobAndUpdateReference(ReflectionUtils.PropertyRef<LargeBlob> blobPropRef, string partitionKey, string rowKey)
        {
            var containerClient = GetContainerClient();
            var blobPath = string.Join("/",
                               _configuration.StorageTableName,
                               partitionKey,
                               rowKey,
                               blobPropRef.FlattenedPropertyName,
                               blobPropRef.StoredInstance.Filename);

            var dataStream = await blobPropRef.StoredInstance.AsyncDataStream.Value;
            var blobClient = containerClient.GetBlobClient(blobPath);
            await blobClient.UploadAsync(dataStream);
            // Should we compare the hashes just in case?
            dataStream.Seek(0, SeekOrigin.Begin);
            var props = await blobClient.GetPropertiesAsync();
            blobPropRef.StoredInstance.Length = props.Value.ContentLength;
            blobPropRef.StoredInstance.ContentType = props.Value.ContentType;
        }

        private async Task<Stream> GetBlobStreamFromAzureBlobStorage(string rowKey, string partitionKey, string flattenedPropertyName, string filename)
        {
            var containerClient = GetContainerClient();
            var blobPath = string.Join("/",
                _configuration.StorageTableName,
                partitionKey,
                rowKey,
                flattenedPropertyName,
                filename);

            var blobClient = containerClient.GetBlobClient(blobPath);

            try
            {
                var downloadRequestResult = await blobClient.DownloadAsync();
                return downloadRequestResult.Value.Content;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreException($"Failed to initiate blob download for blob '{blobPath}': " + e.Message, 
                    AzureTableDataStoreException.ProblemSourceType.BlobStorage, e);
            }
        }

        private async Task InsertMultipleAsync(TData[] entities, bool allowReplace)
        {
            var failedEntries = new ConcurrentDictionary<TData, Exception>();

            // TODO any nice way to figure out a proper number of async calls?
            
            // Run multiple concurrent inserts
            var batches = entities.SplitToBatches(25).ToList();

            foreach (var batch in batches)
            {
                var inserts = batch.Select(async x =>
                {
                    try
                    {
                        await InsertOneAsync(x, allowReplace);
                    }
                    catch (Exception e)
                    {
                        failedEntries.TryAdd(x, e);
                    }
                });
                await Task.WhenAll(inserts);
            }

            if (failedEntries.Any())
            {
                var exception = new AzureTableDataStoreException($"Failed to insert {failedEntries.Count} entities. " +
                    $"See AzureTableDataStoreException.EntityErrors for details.", AzureTableDataStoreException.ProblemSourceType.General,
                    null);
                exception.EntityErrors = failedEntries.ToDictionary(x => (object)x.Key, x => x.Value);
                throw exception;
            }
            

        }

        private async Task InsertBatchedAsync(TData[] entities, bool allowReplace)
        {
            // Form batches of the data to insert.
            // - Only entities without blob references are eligible since this is a transaction (all or nothing) and blobs fall outside that.
            // - Group by partition key
            // - Max 100 items per batch
            // - A batch may not exceed 4MB

            const long maxBatchSize = 4_000_000;

            var blobProperties = ReflectionUtils.GatherPropertiesWithBlobsRecursive(typeof(TData), EntityPropertyConverterOptions);
            if(blobProperties.Any())
                throw new AzureTableDataStoreException("Batched inserts are not supported for entity types with LargeBlob properties due to the " +
                    "transactional nature of Table batch inserts. Please set the useBatching parameter to false.", 
                    AzureTableDataStoreException.ProblemSourceType.Data);

            var entityPartitionGroups = entities.GroupBy(x => _entityTypePartitionKeyPropertyInfo.GetValue(x))
                .ToDictionary(x => x.Key, x => x.ToList());

            var entityBatches = new List<List<DynamicTableEntity>>();

            foreach (var group in entityPartitionGroups)
            {
                var entityBatch = new List<DynamicTableEntity>();
                entityBatches.Add(entityBatch);
                long batchSize = SerializationUtils.CalculateApproximateBatchRequestSize();

                foreach (var entity in group.Value)
                {
                    var collectionPropertyRefs =
                        ReflectionUtils.GatherPropertiesWithCollectionsRecursive(entity, EntityPropertyConverterOptions);
                    
                    var entityKeys = GetEntityKeys(entity);

                    StripSpeciallyHandledProperties(collectionPropertyRefs);
                    var propertyDictionary = EntityPropertyConverter.Flatten(entity, EntityPropertyConverterOptions, null);
                    RestoreSpeciallyHandledProperties(collectionPropertyRefs);

                    propertyDictionary.Remove(_configuration.RowKeyProperty);
                    propertyDictionary.Remove(_configuration.PartitionKeyProperty);

                    collectionPropertyRefs.ForEach(@ref =>
                        propertyDictionary.Add(@ref.FlattenedPropertyName, EntityProperty.GeneratePropertyForString(
                            JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                    var tableEntity = new DynamicTableEntity()
                    {
                        PartitionKey = entityKeys.partitionKey,
                        RowKey = entityKeys.rowKey,
                        ETag = "*",
                        Properties = propertyDictionary
                    };

                    var entitySizeInBytes = SerializationUtils.CalculateApproximateBatchEntitySize(tableEntity);
                    if (batchSize + entitySizeInBytes < maxBatchSize && entityBatch.Count < 100)
                    {
                        entityBatch.Add(tableEntity);
                        batchSize += entitySizeInBytes;
                    }
                    else
                    {
                        entityBatch = new List<DynamicTableEntity> {tableEntity};
                        entityBatches.Add(entityBatch);
                        batchSize = SerializationUtils.CalculateApproximateEntitySize(tableEntity);
                    }
                }
            }

            try
            {
                var batchInsertTasks = entityBatches.Select(x =>
                {
                    var tableRef = GetTable();
                    var batchOp = new TableBatchOperation();
                    x.ForEach(e => batchOp.Add(allowReplace ? TableOperation.InsertOrReplace(e) : TableOperation.Insert(e, true)));
                    return tableRef.ExecuteBatchAsync(batchOp);
                });

                // TODO: limit parallel batch inserts to a sane number if the insert amounts are staggering.
                var results = await Task.WhenAll(batchInsertTasks);
            }
            catch (StorageException e)
            {
                throw new AzureTableDataStoreException($"Batch insert failed: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreException($"Batch insert failed: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.General, e);
            }
            
        }

        public async Task InsertOrReplaceAsync(bool useBatching, params TData[] entities)
        {
            if (entities == null || entities.Length == 0)
                return;

            switch (entities.Length)
            {
                case 1:
                    await InsertOneAsync(entities[0], true);
                    break;
                default:
                    if (useBatching) await InsertBatchedAsync(entities, true);
                    else await InsertMultipleAsync(entities, true);
                    return;
            }
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
                    throw new AzureTableDataStoreException(
                        "Failed to resolve any properties from select expression",
                        AzureTableDataStoreException.ProblemSourceType.Data);
                }
            }
            catch (AzureTableDataStoreException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreException("Failed to convert expression to member name selection: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.Data, e);
            }

            // Automatically warn if this happens: the user might accidentally attempt to change values that are keys, and we should let them know.
            if (mergedPropertyNames.Contains(_configuration.RowKeyProperty) ||
                mergedPropertyNames.Contains(_configuration.PartitionKeyProperty))
                throw new AzureTableDataStoreException(
                    "Do not select the properties for PartitionKey or RowKey in a merge operation; " +
                    "they are immutable, and will automatically be used as the key to update the entity.",
                    AzureTableDataStoreException.ProblemSourceType.Data);

            return mergedPropertyNames;
        }

        public async Task MergeAsync(bool useBatching, Expression<Func<TData, object>> selectMergedPropertiesExpression, params TData[] entities)
        {
            try
            {
                if (entities == null || entities.Length == 0)
                    return;

                List<string> mergedPropertyNames = ValidateSelectExpressionAndExtractMembers(selectMergedPropertiesExpression);

                switch (entities.Length)
                {
                    case 1:
                        await MergeOneAsync(mergedPropertyNames, new DataStoreEntity<TData>("*", entities.First()));
                        break;
                    default:
                        if (useBatching) await MergeBatchedAsync(mergedPropertyNames, entities.Select(x => new DataStoreEntity<TData>("*", x)).ToArray());
                        else await MergeMultipleAsync(mergedPropertyNames, entities.Select(x => new DataStoreEntity<TData>("*", x)).ToArray());
                        return;
                }
            }
            catch (AzureTableDataStoreException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreException("Merge operation(s) failed: " + e.Message, 
                    AzureTableDataStoreException.ProblemSourceType.General, e);
            }

        }

        private async Task MergeOneAsync(List<string> propertyNames, DataStoreEntity<TData> entity)
        {
            try
            {
                var entityKeys = GetEntityKeys(entity.Value);
                var entityData = ExtractEntityProperties(entity.Value);
                var tableRef = GetTable();

                var blobPropertiesToUpdate = entityData.BlobPropertyRefs
                    .Where(x => propertyNames.Contains(x.FlattenedPropertyName))
                    .ToList();

                var collectionPropertiesToUpdate = entityData.CollectionPropertyRefs
                    .Where(x => propertyNames.Contains(x.FlattenedPropertyName))
                    .ToList();

                // Check if the row already exists if we have blobs to upload - we don't want to upload them
                // if the table operation itself may fail.
                if (blobPropertiesToUpdate.Any())
                {
                    TableOperation existsOp = TableOperation.Retrieve(entityKeys.partitionKey, entityKeys.rowKey, new List<string>());
                    try
                    {
                        await tableRef.ExecuteAsync(existsOp);
                    }
                    catch (StorageException e)
                    {
                        if (e.RequestInformation.HttpStatusCode == 404)
                        {
                            throw new AzureTableDataStoreException("An entity with partition key " +
                                $"'{entityKeys.partitionKey}', row key '{entityKeys.rowKey}' does not exist: cannot merge",
                                AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
                        }
                        else
                        {
                            throw new AzureTableDataStoreException("Failed to check the existence of entity with partition key " +
                                $"'{entityKeys.partitionKey}', row key '{entityKeys.rowKey}': cannot merge",
                                AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
                        }
                    }
                    
                    var uploadTasks = blobPropertiesToUpdate
                        .Select(x => UploadBlobAndUpdateReference(x, entityKeys.partitionKey, entityKeys.rowKey)).ToArray();
                    await Task.WhenAll(uploadTasks);

                }

                collectionPropertiesToUpdate.ForEach(@ref =>
                    entityData.PropertyDictionary.Add(@ref.FlattenedPropertyName, EntityProperty.GeneratePropertyForString(
                        JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                blobPropertiesToUpdate.ForEach(@ref =>
                    entityData.PropertyDictionary.Add(@ref.FlattenedPropertyName, EntityProperty.GeneratePropertyForString(
                        JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                var selectedPropertyValues = new Dictionary<string, EntityProperty>();
                foreach (var propertyName in propertyNames)
                {
                    var property = entityData.PropertyDictionary[propertyName];
                    selectedPropertyValues.Add(propertyName, property);
                }

                var tableEntity = new DynamicTableEntity(entityKeys.partitionKey, entityKeys.rowKey, entity.ETag,
                    selectedPropertyValues);

                var mergeOp = TableOperation.Merge(tableEntity);
                
                await tableRef.ExecuteAsync(mergeOp);

            }
            catch (AzureTableDataStoreException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreException("Merge operation(s) failed: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.General, e);
            }
        }

        private async Task MergeMultipleAsync(List<string> propertyNames, DataStoreEntity<TData>[] entities)
        {
            var failedEntities = new ConcurrentDictionary<TData, Exception>();

            // TODO any nice way to figure out a proper number of async calls?

            // Run multiple concurrently
            var batches = entities.SplitToBatches(25).ToList();

            foreach (var batch in batches)
            {
                var merges = batch.Select(async x =>
                {
                    try
                    {
                        await MergeOneAsync(propertyNames, x);
                    }
                    catch (Exception e)
                    {
                        failedEntities.TryAdd(x.Value, e);
                    }
                });
                await Task.WhenAll(merges);
            }

            if (failedEntities.Any())
            {
                var exception = new AzureTableDataStoreException($"Failed to merge {failedEntities.Count} entities. " +
                    $"See AzureTableDataStoreException.EntityErrors for details.", AzureTableDataStoreException.ProblemSourceType.General,
                    null);
                exception.EntityErrors = failedEntities.ToDictionary(x => (object)x.Key, x => x.Value);
                throw exception;
            }

        }

        private async Task MergeBatchedAsync(List<string> propertyNames, DataStoreEntity<TData>[] entities)
        {

            // Form batches of the data to insert.
            // - If attempting to update a LargeBlob property, throw.
            // - Group by partition key
            // - Max 100 items per batch
            // - A batch may not exceed 4MB

            const long maxBatchSize = 4_000_000;
            
            var entityPartitionGroups = entities.GroupBy(x => _entityTypePartitionKeyPropertyInfo.GetValue(x.Value))
                .ToDictionary(x => x.Key, x => x.ToList());

            var entityBatches = new List<List<DynamicTableEntity>>();
            bool haveCheckedForBlobProperties = false;

            foreach (var group in entityPartitionGroups)
            {
                var entityBatch = new List<DynamicTableEntity>();
                entityBatches.Add(entityBatch);
                long batchSize = SerializationUtils.CalculateApproximateBatchRequestSize();

                foreach (var entity in group.Value)
                {
                    var entityKeys = GetEntityKeys(entity.Value);
                    var entityData = ExtractEntityProperties(entity.Value);

                    if (!haveCheckedForBlobProperties)
                    {
                        haveCheckedForBlobProperties = true;
                        foreach (var propertyName in propertyNames)
                        {
                            if (entityData.BlobPropertyRefs.Any(x => x.FlattenedPropertyName == propertyName))
                                throw new AzureTableDataStoreException(
                                    "Batched merge operations do not support updating LargeBlob fields.",
                                    AzureTableDataStoreException.ProblemSourceType.Data, null);
                        }
                    }

                    var selectedPropertyValues = new Dictionary<string, EntityProperty>();

                    entityData.CollectionPropertyRefs.ForEach(@ref =>
                        entityData.PropertyDictionary.Add(@ref.FlattenedPropertyName, EntityProperty.GeneratePropertyForString(
                            JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                    foreach (var propertyName in propertyNames)
                    {
                        var property = entityData.PropertyDictionary[propertyName];
                        selectedPropertyValues.Add(propertyName, property);
                    }

                    var tableEntity = new DynamicTableEntity()
                    {
                        PartitionKey = entityKeys.partitionKey,
                        RowKey = entityKeys.rowKey,
                        ETag = entity.ETag,
                        Properties = selectedPropertyValues
                    };

                    var entitySizeInBytes = SerializationUtils.CalculateApproximateBatchEntitySize(tableEntity);
                    if (batchSize + entitySizeInBytes < maxBatchSize && entityBatch.Count < 100)
                    {
                        entityBatch.Add(tableEntity);
                        batchSize += entitySizeInBytes;
                    }
                    else
                    {
                        entityBatch = new List<DynamicTableEntity> { tableEntity };
                        entityBatches.Add(entityBatch);
                        batchSize = SerializationUtils.CalculateApproximateEntitySize(tableEntity);
                    }
                }
            }

            try
            {
                var batchTasks = entityBatches.Select(x =>
                {
                    var tableRef = GetTable();
                    var batchOp = new TableBatchOperation();
                    x.ForEach(e => batchOp.Add(TableOperation.Merge(e)));
                    return tableRef.ExecuteBatchAsync(batchOp);
                });

                // TODO: limit parallel batch inserts to a sane number if the insert amounts are staggering.
                var results = await Task.WhenAll(batchTasks);
            }
            catch (StorageException e)
            {
                throw new AzureTableDataStoreException($"Batch merge failed: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreException($"Batch merge failed: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.General, e);
            }

        }

        public async Task MergeAsync(bool useBatching, Expression<Func<TData, object>> selectMergedPropertiesExpression, params DataStoreEntity<TData>[] entities)
        {
            try
            {
                if (entities == null || entities.Length == 0)
                    return;

                List<string> mergedPropertyNames = ValidateSelectExpressionAndExtractMembers(selectMergedPropertiesExpression);

                switch (entities.Length)
                {
                    case 1:
                        await MergeOneAsync(mergedPropertyNames, entities.First());
                        break;
                    default:
                        if (useBatching) await MergeBatchedAsync(mergedPropertyNames, entities);
                        else await MergeMultipleAsync(mergedPropertyNames, entities);
                        return;
                }
            }
            catch (AzureTableDataStoreException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreException("Merge operation(s) failed: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.General, e);
            }
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
            ) ExtractEntityProperties(TData entity)
        {
            var blobPropertyRefs =
                ReflectionUtils.GatherPropertiesWithBlobsRecursive(entity, EntityPropertyConverterOptions);
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

        private async Task<TData> GetAsyncInternal(Expression queryExpression)
        {
            string filterString;
            try
            {
                filterString = AzureStorageQueryTranslator.TranslateExpression(queryExpression,
                    _configuration.PartitionKeyProperty, _configuration.RowKeyProperty, EntityPropertyConverterOptions);
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreException("Failed to translate expression into a query: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.Data, e);
            }

            var tableRef = GetTable();
            var query = new TableQuery { FilterString = filterString, TakeCount = 1 };

            try
            {
                var results = await tableRef.ExecuteQuerySegmentedAsync(query, TransformQueryResult, null);
                return results.Results.First();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404)
                    throw new AzureTableDataStoreException($"No matching entity using query '{queryExpression}' was found.",
                        AzureTableDataStoreException.ProblemSourceType.TableStorage, e);

                throw new AzureTableDataStoreException($"Failed to retrieve entities with query '{queryExpression}': " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
            }
        }
        
        // todo only get specified fields if provided
        public async Task<TData> GetAsync(Expression<Func<TData, bool>> queryExpression)
        {
            return await GetAsyncInternal(queryExpression);
        }


        public async Task<TData> GetAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression)
        {
            return await GetAsyncInternal(queryExpression);
        }



        private async Task<DataStoreEntity<TData>> GetWithMetadataAsyncInternal(Expression queryExpression)
        {
            string filterString;
            try
            {
                filterString = AzureStorageQueryTranslator.TranslateExpression(queryExpression,
                    _configuration.PartitionKeyProperty, _configuration.RowKeyProperty, EntityPropertyConverterOptions);
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreException("Failed to translate expression into a query: " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.Data, e);
            }

            var tableRef = GetTable();
            var query = new TableQuery { FilterString = filterString, TakeCount = 1 };

            try
            {
                var results = await tableRef.ExecuteQuerySegmentedAsync(query, null);
                var firstFound = results.First();
                var entity = TransformQueryResult(firstFound.PartitionKey, firstFound.RowKey, firstFound.Timestamp,
                    firstFound.Properties, firstFound.ETag);
                var outputEntity = new DataStoreEntity<TData>()
                {
                    ETag = firstFound.ETag,
                    Timestamp = firstFound.Timestamp,
                    Value = entity
                };
                return outputEntity;
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 404)
                    throw new AzureTableDataStoreException($"No matching entity using query '{queryExpression}' was found.",
                        AzureTableDataStoreException.ProblemSourceType.TableStorage, e);

                throw new AzureTableDataStoreException($"Failed to retrieve entities with query '{queryExpression}': " + e.Message,
                    AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
            }
        }

        public async Task<DataStoreEntity<TData>> GetWithMetadataAsync(Expression<Func<TData, bool>> queryExpression)
        {
            return await GetWithMetadataAsyncInternal(queryExpression);
        }

        public async Task<DataStoreEntity<TData>> GetWithMetadataAsync(
            Expression<Func<TData, DateTimeOffset, bool>> queryExpression)
        {
            return await GetWithMetadataAsyncInternal(queryExpression);
        }


        private TData TransformQueryResult(string partitionKey,
            string rowKey,
            DateTimeOffset timestamp,
            IDictionary<string, EntityProperty> properties,
            string etag)
        {
            properties.Add(_configuration.RowKeyProperty, EntityProperty.CreateEntityPropertyFromObject(rowKey));
            properties.Add(_configuration.PartitionKeyProperty, EntityProperty.CreateEntityPropertyFromObject(partitionKey));

            var blobRefProperties =
                ReflectionUtils.GatherPropertiesWithBlobsRecursive(typeof(TData), EntityPropertyConverterOptions);
            var collRefProperties =
                ReflectionUtils.GatherPropertiesWithCollectionsRecursive(typeof(TData), EntityPropertyConverterOptions);

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
                var filename = deserializedValue.Filename;

                deserializedValue.AsyncDataStream = new Lazy<Task<Stream>>(() => GetBlobStreamFromAzureBlobStorage(rowKey, partitionKey, flattenedPropName, filename));
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

            return converted;

        }

        private object CreateObjectHierarchyForProperty(object rootObject, string flattenedPropName)
        {
            var propertyPath = flattenedPropName.Split(
                new string[] {EntityPropertyConverterOptions.PropertyNameDelimiter}, StringSplitOptions.None);

            object current = rootObject;
            for (var i = 0; i < propertyPath.Length-1; i++)
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