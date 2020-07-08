using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Microsoft.Azure.Cosmos.Table;
using Microsoft.Azure.Cosmos.Tables.SharedFiles;
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

        internal static long CalculateBase64EncodedSize(byte[] input)
        {
            return (long)Math.Ceiling(input.Length  / 3.0M)*4;
        }

        internal static long CalculateBase64EncodedSize(Stream input)
        {
            return (long)Math.Ceiling(input.Length / 3.0M) * 4;
        }
    }
}