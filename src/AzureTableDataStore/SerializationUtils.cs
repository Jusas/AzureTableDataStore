using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json.Serialization;

namespace AzureTableDataStore
{
    internal class SerializationUtils
    {
        // Limitations:
        // - Entity size is max 1MB
        // - Entities in transaction max 100
        // - Entities in transaction must have same partition key
        // - Transaction max size 4MB

        internal static readonly JsonSerializerSettings JsonSerializerSettings = new JsonSerializerSettings()
        {
            ContractResolver = new DefaultContractResolver(),
            NullValueHandling = NullValueHandling.Ignore,
            DateParseHandling = DateParseHandling.None
        };
        internal static readonly JsonSerializer JsonSerializer = JsonSerializer.Create(JsonSerializerSettings);

        /// <summary>
        /// This method calculates the serialized size of the DynamicTableEntities.
        /// The method of serialization should be very much identical to how Microsoft.Azure.Cosmos.Table serializes
        /// the data to requests, which we just have to do in order to get an accurate enough
        /// size of the data about to be sent. This data can then be used to ensure that batched
        /// requests don't exceed the request size limits.
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        internal static long CalculateApproximateEntitySize(params DynamicTableEntity[] entities)
        {
            if (entities.Length == 0)
                return 0;

            using (var streamWriter = new StreamWriter(new MemoryStream()))
            {
                using (JsonTextWriter jsonWriter = new JsonTextWriter(streamWriter))
                {
                    jsonWriter.CloseOutput = false;

                    foreach (var entity in entities)
                    {
                        Dictionary<string, object> serializedValues = new Dictionary<string, object>
                        {
                            { "PartitionKey", entity.PartitionKey },
                            { "RowKey", entity.RowKey }
                        };

                        foreach (var kvp in entity.Properties)
                        {
                            if (kvp.Value != null && kvp.Value.PropertyAsObject != null)
                            {
                                var key = kvp.Key;
                                if (kvp.Value.PropertyType == EdmType.DateTime)
                                {
                                    var dt = (DateTime)kvp.Value.DateTime;
                                    serializedValues.Add(key, dt.ToString("o", CultureInfo.InvariantCulture));
                                    serializedValues.Add(key + "@odata.type", "Edm.DateTime");
                                }
                                else if (kvp.Value.PropertyType == EdmType.Binary)
                                {
                                    serializedValues.Add(key, Convert.ToBase64String(kvp.Value.BinaryValue));
                                    serializedValues.Add(key + "@odata.type", "Edm.Binary");
                                }
                                else if (kvp.Value.PropertyType == EdmType.Int64)
                                {
                                    serializedValues.Add(key, kvp.Value.Int64Value.ToString());
                                    serializedValues.Add(key + "@odata.type", "Edm.Int64");
                                }
                                else if (kvp.Value.PropertyType == EdmType.Guid)
                                {
                                    serializedValues.Add(key, kvp.Value.GuidValue.ToString());
                                    serializedValues.Add(key + "@odata.type", "Edm.Guid");
                                }
                                else
                                    serializedValues.Add(key, kvp.Value.PropertyAsObject);
                            }
                        }

                        JObject.FromObject(serializedValues, JsonSerializer).WriteTo(jsonWriter);
                    }
                    jsonWriter.Flush();
                    return streamWriter.BaseStream.Length;
                }
            }

        }

        /// <summary>
        /// An approximation of how big the batch request is going to be.
        /// As we roughly know how big the header data is, what is transmitted per entity and what
        /// is left in the request footer we can calculate the batch size. It isn't 100% correct,
        /// but close enough.
        /// </summary>
        /// <param name="entities"></param>
        /// <returns></returns>
        internal static long CalculateApproximateBatchRequestSize(params DynamicTableEntity[] entities)
        {
            try
            {
                // Request header size, roughly (batch, content-type with some added padding):
                long headerSize = 150;

                // Request footer size, roughly (changeset and batch and some added padding):
                long footerSize = 120;

                // The entities inside the batch request:
                long entitiesSize = CalculateApproximateEntitySize(entities);

                // Headers per entity:
                long entityWrappings = entities.Length * 500;

                return headerSize + footerSize + entityWrappings + entitiesSize;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreInternalException("Failed to calculate approximate batch request size: " + e.Message, e);
            }
            
        }

        internal static long CalculateApproximateBatchEntitySize(DynamicTableEntity entity)
        {
            try
            {
                long entitiesSize = CalculateApproximateEntitySize(entity);
                long entityWrappings = 500;
                return entitiesSize + entityWrappings;
            }
            catch (Exception e)
            {
                throw new AzureTableDataStoreInternalException("Failed to calculate approximate entity batch size: " + e.Message, e);
            }
        }

        static readonly DateTime _minDateTime = new DateTime(1601, 1, 1);
        static readonly DateTime _maxDateTime = new DateTime(9999, 12, 31);

        internal static List<string> ValidateBlobPath(string blobPath)
        {
            // A blob name can contain any combination of characters.
            // A blob name must be at least one character long and cannot be more than 1,024 characters long, for blobs in Azure Storage.
            // The Azure Storage emulator supports blob names up to 256 characters long.
            // Blob names are case-sensitive.
            // Reserved URL characters must be properly escaped.
            // The number of path segments comprising the blob name cannot exceed 254.
            // A path segment is the string between consecutive delimiter characters (e.g., the forward slash '/')
            // that corresponds to the name of a virtual directory.
            // Avoid blob names that end with a dot (.), a forward slash (/), or a sequence or combination of the two.

            var errors = new List<string>();

            if(blobPath.Length > 1024)
                errors.Add("Blob path exceeds 1024 characters (note that table name, partition and row keys are included in the blob path)");
            if(blobPath.EndsWith(".") || blobPath.EndsWith("/"))
                errors.Add("Blob path should not end with '.' or '/'");

            return errors;
        }

        internal static List<string> ValidateProperties(DynamicTableEntity entity)
        {
            // Note: Storage Emulator appears to have a different set of rules. 
            // https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator#differences-between-the-storage-emulator-and-azure-storage
            // Therefore these should be tested with an actual storage account.

            var errors = new List<string>();
            if (string.IsNullOrEmpty(entity.PartitionKey))
                errors.Add("Partition key is not set");
            if (string.IsNullOrEmpty(entity.RowKey))
                errors.Add("Row key is not set");
            
            if (entity.Properties.Count > 252)
                errors.Add("Entity may only contain 252 properties (255 - rowkey, partitionkey, timestamp)");
            if (Encoding.Unicode.GetByteCount(entity.PartitionKey ?? "") > 1024) 
                errors.Add("Partition key length cannot exceed 1024 bytes");
            if (Encoding.Unicode.GetByteCount(entity.RowKey ?? "") > 1024)
                errors.Add("Row key length cannot exceed 1024 bytes");

            var allowedKeyRegexPattern = @"[\x00-\x1F\x7f-\x9F\/\\\#\?]";
            if (Regex.IsMatch(entity.PartitionKey ?? "", allowedKeyRegexPattern))
                errors.Add("Partition key contains illegal characters");
            if (Regex.IsMatch(entity.RowKey ?? "", allowedKeyRegexPattern))
                errors.Add("Row key contains illegal characters");


            int totalDataLength = Encoding.Unicode.GetByteCount(entity.PartitionKey ?? "") + Encoding.Unicode.GetByteCount(entity.RowKey ?? "");

            foreach (var prop in entity.Properties)
            {
                if(prop.Key.Length > 255)
                    errors.Add($"'{prop.Key}': Property key after serialization exceeds 255 characters");
                if(prop.Value.PropertyType == EdmType.String && Encoding.Unicode.GetByteCount(prop.Value.StringValue) > 65536)
                    errors.Add($"'{prop.Key}': Property size exceeds 65536 bytes (UTF-16 encoding)");
                else if(prop.Value.PropertyType == EdmType.Binary && prop.Value.BinaryValue.Length > 65536)
                    errors.Add($"'{prop.Key}': Property binary value exceeds 65536 bytes");
                else if(prop.Value.PropertyType == EdmType.DateTime && prop.Value.DateTime != null && (prop.Value.DateTime < _minDateTime || prop.Value.DateTime > _maxDateTime))
                    errors.Add($"'{prop.Key}': DateTime value must be between 1601-01-01 and 9999-12-31");

                totalDataLength += GetSize(prop.Value);
            }

            if(totalDataLength > 1048576)
                errors.Add("Collective entity size exceeds 1 MiB");

            return errors;
        }

        internal static int GetSize(EntityProperty property)
        {
            switch (property.PropertyType)
            {
                case EdmType.Binary:
                    return property.BinaryValue.Length;
                case EdmType.DateTime:
                    return 8;
                case EdmType.Boolean:
                    return 1;
                case EdmType.Double:
                    return 8;
                case EdmType.Guid:
                    return 16;
                case EdmType.Int32:
                    return 4;
                case EdmType.Int64:
                    return 16;
                case EdmType.String:
                    return Encoding.Unicode.GetByteCount(property.StringValue);
            }

            return 0;
        }

    }
}