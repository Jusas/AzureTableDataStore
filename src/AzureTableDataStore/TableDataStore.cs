using System;
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

        public async Task InsertAsync(bool useBatching, params TData[] entries)
        {
            switch (entries?.Length)
            {
                case 0:
                    return;
                case 1:
                    await InsertOneAsync(entries[0]);
                    break;
                default:
                    if (useBatching) await InsertBatched(entries);
                    else await InsertMultiple(entries);
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
                        throw new AzureTableDataStoreException("Unable to initialize blob container (CreateIfNotExists)",
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
                    throw new AzureTableDataStoreException("Unable to initialize table (CreateIfNotExists)",
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


        private async Task InsertOneAsync(TData entity)
        {
            try
            {
                var blobPropertyRefs =
                    ReflectionUtils.GatherPropertiesWithBlobsRecursive(entity, EntityPropertyConverterOptions);
                var collectionPropertyRefs =
                    ReflectionUtils.GatherPropertiesWithCollectionsRecursive(entity, EntityPropertyConverterOptions);
                var allSpecialPropertyRefs = blobPropertyRefs.Cast<ReflectionUtils.PropertyRef>()
                    .Concat(collectionPropertyRefs).ToList();

                StripSpeciallyHandledProperties(allSpecialPropertyRefs);
                var propertyDictionary = EntityPropertyConverter.Flatten(entity, EntityPropertyConverterOptions, null);

                propertyDictionary.Remove(_configuration.RowKeyProperty);
                propertyDictionary.Remove(_configuration.PartitionKeyProperty);

                var entryKeys = GetEntityKeys(entity);

                var tableRef = GetTable();

                // Check if the row already exists if we have blobs to upload - we don't want to upload them
                // if the table insert itself may fail.
                if (blobPropertyRefs.Any())
                {
                    TableOperation existsOp = TableOperation.Retrieve(entryKeys.partitionKey, entryKeys.rowKey, new List<string>());
                    try
                    {
                        await tableRef.ExecuteAsync(existsOp);
                        throw new AzureTableDataStoreException("Entity with partition key " +
                            $"'{entryKeys.partitionKey}', row key '{entryKeys.rowKey}' already exists, cannot insert.",
                            AzureTableDataStoreException.ProblemSourceType.TableStorage);
                    }
                    catch (StorageException e)
                    {
                        if (e.RequestInformation.HttpStatusCode != 404)
                        {
                            throw new AzureTableDataStoreException("Failed to check if entity exists with partition key " +
                                $"'{entryKeys.partitionKey}', row key '{entryKeys.rowKey}'", 
                                AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
                        }
                    }
                }
                
                var uploadTasks = blobPropertyRefs
                    .Select(x => UploadBlobAndUpdateReference(x, entryKeys.partitionKey, entryKeys.rowKey)).ToArray();
                await Task.WhenAll(uploadTasks);

                collectionPropertyRefs.ForEach(@ref =>
                    propertyDictionary.Add(@ref.FlattenedPropertyName, EntityProperty.GeneratePropertyForString(
                        JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                blobPropertyRefs.ForEach(@ref =>
                    propertyDictionary.Add(@ref.FlattenedPropertyName, EntityProperty.GeneratePropertyForString(
                        JsonConvert.SerializeObject(@ref.StoredInstance, _jsonSerializerSettings))));

                RestoreSpeciallyHandledProperties(allSpecialPropertyRefs);

                var tableEntity = new DynamicTableEntity(entryKeys.partitionKey, entryKeys.rowKey, "*",
                    propertyDictionary);

                var insertOp = TableOperation.Insert(tableEntity);
                await tableRef.ExecuteAsync(insertOp);

            }
            catch (AzureTableDataStoreException)
            {
                throw;
            }
            catch (SerializationException e)
            {
                throw new AzureTableDataStoreException("Serialization of the data failed", 
                    AzureTableDataStoreException.ProblemSourceType.Data, e);
            }
            catch (Exception e)
            {
                if(e.GetType().Namespace.StartsWith("Microsoft.Azure.Cosmos"))
                    throw new AzureTableDataStoreException("Insert operation failed, outlying Table Storage threw an exception: " + e.Message, 
                        AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
                if(e.GetType().Namespace.StartsWith("Azure.Storage") || e is Azure.RequestFailedException)
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

        private async Task InsertMultiple(TData[] entries)
        {
            // todo
        }

        private async Task InsertBatched(TData[] entities)
        {
            // Form batches of the data to insert.
            // - Only entities without blob references are eligible since this is a transaction (all or nothing) and blobs fall outside that.
            // - Group by partition key
            // - Max 100 items per batch
            // - A batch may not exceed 4MB

            const long maxBatchSize = 4_000_000;

            var blobProperties = ReflectionUtils.GatherPropertiesWithBlobsRecursive(typeof(TData), EntityPropertyConverterOptions);
            if(blobProperties.Any())
                throw new AzureTableDataStoreException("Batched inserts are not supported for entity types with blob (LargeBlob) properties due to the " +
                    "transactional nature of Table batch inserts. Please set the useBatching parameter to false.");

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
                    x.ForEach(e => batchOp.Add(TableOperation.Insert(e, true)));
                    return tableRef.ExecuteBatchAsync(batchOp);
                });

                await Task.WhenAll(batchInsertTasks);
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

        public Task UpsertAsync(bool useBatching, params TData[] entries)
        {
            throw new NotImplementedException();
        }

        public Task MergeAsync(bool useBatching, Expression<Func<TData, object>> selectExpression, params TData[] entries)
        {
            throw new NotImplementedException();
        }

        public Task<IList<TData>> FindAsync(Expression<Func<TData, bool>> queryExpression)
        {
            throw new NotImplementedException();
        }

        public Task<IList<TData>> FindAsync(string query)
        {
            throw new NotImplementedException();
        }

        // todo only get specified fields if provided
        public async Task<TData> GetAsync(Expression<Func<TData, bool>> queryExpression)
        {
            var filterString = AzureStorageQueryTranslator.TranslateExpression(queryExpression, 
                _configuration.PartitionKeyProperty, _configuration.RowKeyProperty);

            var tableRef = GetTable();
            var query = new TableQuery {FilterString = filterString, TakeCount = 1};

            try
            {
                var results = await tableRef.ExecuteQuerySegmentedAsync(query, TransformQueryResult, null);
                return results.Results.First();
            }
            catch (StorageException e)
            {
                if(e.RequestInformation.HttpStatusCode == 404)
                    throw new AzureTableDataStoreException($"No matching entity using query '{queryExpression}' was found.",
                        AzureTableDataStoreException.ProblemSourceType.TableStorage, e);

                throw new AzureTableDataStoreException($"Failed to retrieve entities with query '{queryExpression}': " + e.Message, 
                    AzureTableDataStoreException.ProblemSourceType.TableStorage, e);
            }
            
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

        public Task<TData> GetAsync(string query)
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(params TData[] entries)
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(params string[] ids)
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(Expression<Func<TData, bool>> queryExpression)
        {
            throw new NotImplementedException();
        }

        public Task DeleteAsync(string query)
        {
            throw new NotImplementedException();
        }

        public Task EnumerateAsync(Func<TData, Task> enumeratorFunc)
        {
            throw new NotImplementedException();
        }

        public Task EnumerateAsync(Expression<Func<TData, bool>> queryExpression, Func<TData, Task> enumeratorFunc)
        {
            throw new NotImplementedException();
        }

        public Task EnumerateAsync(string query, Func<TData, Task> enumeratorFunc)
        {
            throw new NotImplementedException();
        }
    }
}