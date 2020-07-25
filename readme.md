# Azure Table Data Store

[![Build Status](https://dev.azure.com/jusasi/AzureTableDataStore/_apis/build/status/Jusas.AzureTableDataStore?branchName=master)](https://dev.azure.com/jusasi/AzureTableDataStore/_build/latest?definitionId=2&branchName=master) [![Azure DevOps coverage](https://img.shields.io/azure-devops/coverage/jusasi/AzureTableDataStore/2/master)](https://dev.azure.com/jusasi/AzureTableDataStore/_build/latest?definitionId=2&branchName=master) [![Wiki](https://img.shields.io/badge/docs-in%20wiki-green.svg?style=flat)](https://github.com/Jusas/AzureTableDataStore/wiki) 

![Logo](graphics/logo.png)

# Introduction

This library was created to provide an easy interface to Azure Storage Tables, working in concert with Azure Blob Storage to seamlessly store blobs related to table rows there.

## NOTE: Work in progress!
    When stable, a Nuget package will be released. Until then, this notification remains.

The **Microsoft.Azure.Cosmos.Table** library already provides a pretty decent interface for table operations but quite often you still have to repeat yourself a lot, and work with queries and more complex table entities can get tedious. In many cases what we really need from the tables is really simple and we'd like to spend as little effort as possible to insert, update, find and iterate over rows but the library forces us to make fairly many calls for somewhat simple use case scenarios. Finally when you add blobs into the mix, things suddenly become a lot more work intensive.

Hence why **AzureTableDataStore** exists: to make it easy and fast to utilize Table and Blob storage in common use cases. Under the hood it uses the Microsoft.Azure.Cosmos.Table to perform the Table operations and Azure.Storage.Blobs to perform the Blob operations.

# Examples

The usage and its simplicity is probably best explained with some examples.

Inserting a few entities is a breeze, and we may utilize batch inserts:

```CS

// Basically any POCO class will do.
public class MyEntity
{
    [TableRowKey]
    public string Id { get; set; }
    [TablePartitionKey]
    public string Group { get; set; }

    public int Value { get; set; }
    public Description Description { get; set; }

    // Collections and dictionaries are now supported too, and are stored as JSON strings.
    public List<string> Tags { get; set; }
}

public class Description 
{
    public string LongDescription { get; set; }
    public string ShortDescription { get; set; }
}

// Make some entities...
var myEntities = new MyEntity[] { ... };

// Create a new TableDataStore for that type.
var store = new TableDataStore<MyEntity>(...);

// Insert as a transactional batch.
await store.InsertAsync(BatchingMode.Strict, myEntities);

```

As a side note, we can define entity model RowKey and PartitionKey using attributes, or if you prefer
you can also provide the key property names in the TableDataStore constructor.

Getting and finding entities is just as easy:

```CS
var item = await store.GetAsync(row => row.Group == "LargeValues" && row.Id == "A1");
var results = await store.FindAsync(row => row.Group == "SmallValues" && row.Value < 100);
```

Merging (updating) fields, in this case updating just the selected fields:

```CS
var item = await store.GetAsync(row => row.Group == "LargeValues" && row.Id == "A1");
item.Description.LongDescription = "Hello Storage!";
item.Description.ShortDescription = "Hello!";

await store.MergeAsync(BatchingMode.None, 
    selectMergedPropertiesExpression: entity => new 
    { 
        entity.Description.LongDescription,
        entity.Description.ShortDescription
    },
    largeBlobNullBehavior: LargeBlobNullBehavior.IgnoreProperty, 
    item);
```

As you just probably noticed, we just mentioned **LargeBlobs** the first time.
LargeBlobs are essentially the references to Azure Blob Storage blobs and defining a property as LargeBlob enables you to attach blobs to your data models. Let's take a look:

```CS
public class EntityWithBlobs
{
    [TableRowKey]
    public string Id { get; set; }
    [TablePartitionKey]
    public string Category { get; set; }

    public string Text { get; set; }
    public LargeBlob Attachment { get; set; }
}

byte[] myData = ...;

var entity = new EntityWithBlobs()
{
    Id = "entity-1",
    Category = "messages",
    Text = "Hello World!",
    Attachment = new LargeBlob("content.txt", myData, "text/plain")
};

await store.InsertAsync(BatchingMode.None, entity);
```

As the table row gets inserted, the blob gets uploaded to the designated Blob container. The path of the blob matches the path of the LargeBlob property. Blob content can be provided as bytes, a Stream, a string or a factory method that produces a Stream, synchronously or asynchronously.

Later on, when you need the blob content, just get it:

```CS

var myEntity = await store.GetAsync(entity => entity.Id == "entity-1");

// Length, filename and content type are always cached in the table.
var myAttachmentFilename = myEntity.Attachment.Filename;
var myAttachmentLength = myEntity.Attachment.Length;

// A Lazy accessor to the Stream gets you the blob content whenever you need it.
using(var myStream = await myEntity.Attachment.AsyncDataStream.Value)
{
    // Read the stream content.
}

// Or maybe you'd like a straight up URL to the blob:
var url = myEntity.Attachment.GetDownloadUrl(
    withSasToken: true, 
    tokenExpiration: TimeSpan.FromHours(24)
);

```

Finally, deletion is a breeze as well, and this also cleans up any blobs that are attached to the entity:

```CS
await store.DeleteAsync(BatchingMode.None, myEntity);
```

# Batching modes

As mentioned, batch operations with Table Storage are fully supported. For the sake of clarity there are 3 (or 4, if you count no batching) different batching modes to choose from and they all act differently. This is so that the user can explicitly choose the right mode for the right job.

## None

No batching is used. Table Entities are processed individually but can be processed in parallel. Inefficient for large amounts of operations but generally the simplest case to handle when you're worrying about errors and error handling. Blob operations are performed only after successful Table operations.

## Strict

The Strict batching mode equals the Azure Table Storage default batch rules:
- Maximum of 100 entities per batch
- All entities in the batch must belong to the same partition
- Entity size may not exceed 1MB
- Batch size may not exceed 4MB

The batch is essentially a transaction, where either all operations succeed or none of them will.
In addition, the entity data model may not contain **LargeBlob** properties (with inserts being an exception when all the property values are set to null). This is because the Strict mode represents the Azure Table Storage batch rules exactly, and it would not be a transaction anymore if there were non-transactionable blob operations involved.

## Strong

Basically the same as Strict, but with the big differences that in Strong mode:
- Any number of entities can be handled, they will be grouped internally to sub-batches
- Entities may belong to any partition, and are grouped internally to sub-batches per partition
- No batch size limitations, since grouping to sub-batches is being performed internally and each sub-batch will be kept under the maximum batch size limit

This is especially useful mode when you have a lot of entity operations and your entities are from different partitions. All the heavy lifting gets done automatically. Sub-batches may however fail, so this is no longer a single transaction.

## Loose

Loose mode combines Table Entities into sub-batches like Strong mode but also allows Blob operations to be performed along with them. This mode is no longer in any meaningful way transactional. Each sub-batch of entities is followed by related blob operations and there is no guarantee that the produced outcome is fully consistent - one or more blob operations could for example fail, while the related Table Entity batch would succeed, leaving some entities potentially in a broken state regarding their blobs.

This mode exists to allow batch operations with data models that contain LargeBlobs but you should be aware of the issues that may come along with it.

# Issues and documentation

See the [Wiki](https://github.com/Jusas/AzureTableDataStore/wiki) for known issues and more detailed documentation and advanced examples.
