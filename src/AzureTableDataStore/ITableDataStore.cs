using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;
// ReSharper disable UnusedMember.Global


namespace AzureTableDataStore
{
    /// <summary>
    /// The name providing interface for ITableDataStore.
    /// </summary>
    public interface INamedTableDataStore
    {
        /// <summary>
        /// The name of the TableDataStore instance, to enable identifying instances of <see cref="ITableDataStore{TData}"/> injected by DI.
        /// </summary>
        string Name { get; }
    }

    /// <summary>
    /// The selected batching mode to use when performing multiple Insert/Merge/Replace operations.
    /// </summary>
    public enum BatchingMode
    {
        /// <summary>
        /// No batching. Perform each operation individually. Multiple operations can be performed in parallel.
        /// </summary>
        None,

        /// <summary>
        /// Strict batching, i.e. Transaction mode.
        /// <para>
        /// All entity operations must fit into a single batch, which will then be executed
        /// as a Transaction "all or nothing". Can be performed for 1-100 entities, and be within Azure Table Storage limit of max. 4MB batch size.
        /// </para>
        /// <para>
        /// As per Azure Table Storage batch rules, all entities in the batch must sit in the same partition.
        /// </para>
        /// <para>
        /// NOTE: Cannot be used with operations that contain entities with <see cref="LargeBlob"/> properties, with
        /// the exception of inserts and merges where strong batching can be used when all <see cref="LargeBlob"/> properties are set to non-null,
        /// and <see cref="LargeBlobNullBehavior"/> is set to <see cref="LargeBlobNullBehavior.IgnoreProperty"/>.
        /// </para>
        /// </summary>
        Strict,

        /// <summary>
        /// Strong batching, i.e. multi-batch Transaction mode with multiple partition keys allowed. In this mode entity operations are performed in sub-batches of 1-100 entities,
        /// and there is no limit on the number of entities.
        /// <para>
        /// Operations are grouped into batches by partition key with 1-100 entities per table operation sub-batch when sent to the Table API, as per Azure Table Storage operation limits.
        /// Each of these operation sub-batches are guaranteed to be "all or nothing", transaction like. Errors are tracked on sub-batch level, and one sub-batch failure does not stop execution.
        /// </para>
        /// <para>
        /// NOTE: Cannot be used with operations that contain entities with <see cref="LargeBlob"/> properties, with
        /// the exception of inserts and merges where strong batching can be used when all <see cref="LargeBlob"/> properties are set to non-null,
        /// and <see cref="LargeBlobNullBehavior"/> is set to <see cref="LargeBlobNullBehavior.IgnoreProperty"/>.
        /// </para>
        /// </summary>
        Strong,

        /// <summary>
        /// Loose batching, which can be used with any number of entities and multiple partition keys are allowed. Performs Table operations in sub-batches of 1-100 entities and each batch is followed by
        /// related Blob Storage operations for <see cref="LargeBlob"/> properties. Errors are tracked on sub-batch level, and one sub-batch failure does not stop execution.
        /// Errors are available in any raised exception.
        /// <para>
        /// Blob operations related to each entity sub-batch are performed after each Table operation sub-batch if the sub-batch operation succeeds. Any of them may fail, in which case
        /// the state of some entities will be left inconsistent. Blob operation errors are tracked on individual <see cref="LargeBlob"/> and entity level and will be available in any raised exception.
        /// </para>
        /// <para>
        /// This mode enables performance at the cost of data integrity.
        /// </para>
        /// </summary>
        Loose
    }

    /// <summary>
    /// Interface for the <see cref="TableDataStore{TData}"/>.
    /// </summary>
    /// <typeparam name="TData">The entity type that is stored in the Azure Storage Table.</typeparam>
    public interface ITableDataStore<TData> : INamedTableDataStore where TData:new()
    {
        /// <summary>
        /// Inserts new entities into Table Storage.
        /// </summary>
        /// <param name="batchingMode">
        /// Chooses the batching mode to use when there are multiple entities.
        /// </param>
        /// <param name="entities">One ore more entities to insert of the <typeparamref name="TData"/> type.</param>
        /// <exception cref="AzureTableDataStoreSingleOperationException{TData}"></exception>
        /// <exception cref="AzureTableDataStoreBatchedOperationException{TData}"></exception>
        /// <exception cref="AzureTableDataStoreMultiOperationException{TData}"></exception>
        /// <returns></returns>
        Task InsertAsync(BatchingMode batchingMode, params TData[] entities);

        /// <summary>
        /// Inserts or replaces entities into Table Storage. When an entity with the same partition and row keys already exists,
        /// it will be replaced.
        /// <para>
        /// If the entity has any <see cref="LargeBlob"/> properties, those will have their old blobs replaced by the new ones. If the filename
        /// changes, the old blob will get deleted. If the new property value is null, then the old blob will get deleted.
        /// </para>
        /// </summary>
        /// <param name="batchingMode">
        /// Chooses the batching mode to use when there are multiple entities.
        /// </param>
        /// <param name="entities">One or more entities to insert/replace</param>
        /// <exception cref="AzureTableDataStoreSingleOperationException{TData}"></exception>
        /// <exception cref="AzureTableDataStoreBatchedOperationException{TData}"></exception>
        /// <exception cref="AzureTableDataStoreMultiOperationException{TData}"></exception>
        /// <returns></returns>
        Task InsertOrReplaceAsync(BatchingMode batchingMode, params TData[] entities);

        /// <summary>
        /// Merges (aka updates) the property values from the provided entities using the properties
        /// selected in the expression.
        /// <para>
        /// NOTE: this method assumes ETag: '*' for all entities, and therefore will overwrite indiscriminately.
        /// </para>
        /// <para>
        /// To use ETags, use <see cref="MergeAsync(BatchingMode,System.Linq.Expressions.Expression{System.Func{TData,object}},LargeBlobNullBehavior,DataStoreEntity{TData}[])"/>
        /// </para>
        /// </summary>
        /// <param name="batchingMode">
        /// Chooses the batching mode to use when there are multiple entities.
        /// </param>
        /// <param name="selectMergedPropertiesExpression">
        /// An expression to select the properties to merge.
        /// <para>Example: entity => new { entity.Name, entity.Email }</para>
        /// </param>
        /// <param name="largeBlobNullBehavior">
        /// <para>
        /// Defines how to interpret null values in <see cref="LargeBlob"/> properties. Defaults to <see cref="LargeBlobNullBehavior.IgnoreProperty"/>.
        /// </para>
        /// <para>
        /// When the entity has any <see cref="LargeBlob"/> properties, and the behavior is set to <see cref="LargeBlobNullBehavior.DeleteBlob"/>,
        /// null values in those properties will translate to their existing blobs getting deleted.
        /// </para>
        /// <para>
        /// If the behavior is set to <see cref="LargeBlobNullBehavior.IgnoreProperty"/> then those properties will be
        /// left untouched by this operation.
        /// </para>
        /// </param>
        /// <param name="entities">The entities to update.</param>
        /// <exception cref="AzureTableDataStoreSingleOperationException{TData}"></exception>
        /// <exception cref="AzureTableDataStoreBatchedOperationException{TData}"></exception>
        /// <exception cref="AzureTableDataStoreMultiOperationException{TData}"></exception>
        /// <returns></returns>
        Task MergeAsync(BatchingMode batchingMode, Expression<Func<TData, object>> selectMergedPropertiesExpression, LargeBlobNullBehavior largeBlobNullBehavior = LargeBlobNullBehavior.IgnoreProperty,
            params TData[] entities);

        /// <summary>
        /// Merges (aka updates) the property values from the provided entities using the properties
        /// selected in the expression.
        /// <para>
        /// Uses optimistic concurrency, i.e. the <see cref="DataStoreEntity{TData}.ETag"/> matching determines whether or not
        /// the merge can be made.
        /// </para>
        /// </summary>
        /// <param name="batchingMode">
        /// Chooses the batching mode to use when there are multiple entities.
        /// </param>
        /// <param name="selectMergedPropertiesExpression">
        /// An expression to select the properties to merge.
        /// <para>
        /// Example: <c>entity => new { entity.UserId, entity.EmployeeType, entity.Contact.Email }</c>
        /// </para>
        /// </param>
        /// <param name="largeBlobNullBehavior">
        /// <para>
        /// Defines how to interpret null values in <see cref="LargeBlob"/> properties. Defaults to <see cref="LargeBlobNullBehavior.IgnoreProperty"/>.
        /// </para>
        /// <para>
        /// When the entity has any <see cref="LargeBlob"/> properties, and the behavior is set to <see cref="LargeBlobNullBehavior.DeleteBlob"/>,
        /// null values in those properties will translate to their existing blobs getting deleted.
        /// </para>
        /// <para>
        /// If the behavior is set to <see cref="LargeBlobNullBehavior.IgnoreProperty"/> then those properties will be
        /// left untouched by this operation.
        /// </para>
        /// </param>
        /// <param name="entities">The entities to update, wrapped into <see cref="DataStoreEntity{TData}"/> objects to provide ETags.</param>
        /// <exception cref="AzureTableDataStoreSingleOperationException{TData}"></exception>
        /// <exception cref="AzureTableDataStoreBatchedOperationException{TData}"></exception>
        /// <exception cref="AzureTableDataStoreMultiOperationException{TData}"></exception>
        /// <returns></returns>
        Task MergeAsync(BatchingMode batchingMode, Expression<Func<TData, object>> selectMergedPropertiesExpression, LargeBlobNullBehavior largeBlobNullBehavior = LargeBlobNullBehavior.IgnoreProperty,
            params DataStoreEntity<TData>[] entities);

        /// <summary>
        /// Finds entities using the provided query expression, and returns a list of the <typeparamref name="TData"/> typed entities.
        /// </summary>
        /// <param name="queryExpression">
        /// The query to use in expression form. The operators ==, !=, &gt;, &gt;=, &lt;, &lt;= and ! are supported.
        /// <para>
        /// Example: <c>entity => (entity.Category == "Worker" &amp;&amp; entity.Money &gt; 9000) || entity.Money &gt;= 5000</c>
        /// </para>
        /// </param>
        /// <param name="selectExpression">
        /// The properties to get and return, as an expression.
        /// <para>
        /// Example: <c>entity => new { entity.UserId, entity.EmployeeType, entity.Contact.Email }</c>
        /// </para>
        /// </param>
        /// <param name="limit">
        /// The maximum number of limits to return. If null, does not limit the number of results.
        /// </param>
        /// <exception cref="AzureTableDataStoreQueryException"></exception>
        Task<IList<TData>> FindAsync(Expression<Func<TData, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null, int? limit = null);

        /// <summary>
        /// Finds entities using the provided query expression, with the additional <see cref="DateTimeOffset"/> parameter representing the entity Timestamp.
        /// </summary>
        /// <param name="queryExpression">
        /// The query to use in expression form. The operators ==, !=, &gt;, &gt;=, &lt;, &lt;= and ! are supported.
        /// The additional DateTimeOffset parameter represents the row Timestamp in the table.
        /// <para>
        /// Example: <c>(entity, timestamp) => entity.UserId == "007" &amp;&amp; timestamp &gt; yesterdayUtc</c>
        /// </para>
        /// </param>
        /// <param name="selectExpression">
        /// The properties to get and populate, as an expression. If null, populates all object properties.
        /// <para>
        /// Example: <c>entity => new { entity.UserId, entity.EmployeeType, entity.Contact.Email }</c>
        /// </para>
        /// </param>
        /// <param name="limit">The maximum number of limits to return. If null, does not limit the number of results.</param>
        /// <returns></returns>
        /// <exception cref="AzureTableDataStoreQueryException"></exception>
        Task<IList<TData>> FindAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null, int? limit = null);

        /// <summary>
        /// Finds entities using the provided query expression and returns a list of the <typeparamref name="TData"/> typed entities wrapped into
        /// <see cref="DataStoreEntity{TData}"/> instances that also contain the row's ETag and Timestamp.
        /// </summary>
        /// <param name="queryExpression">
        /// The query to use in expression form. The operators ==, !=, &gt;, &gt;=, &lt;, &lt;= and ! are supported, as well as the binary &amp;&amp; and || and parenthesis.
        /// <para>
        /// Example: <c>entity => (entity.Category == "Worker" &amp;&amp; entity.Money &gt; 9000) || entity.Money &gt;= 5000</c>
        /// </para>
        /// </param>
        /// <param name="selectExpression">
        /// The properties to get and return, as an expression. If null, populates all object properties.
        /// <para>
        /// Example: <c>entity => new { entity.UserId, entity.EmployeeType, entity.Name }</c>
        /// </para>
        /// </param>
        /// <param name="limit">The maximum number of limits to return. If null, does not limit the number of results.</param>
        /// <returns></returns>
        /// <exception cref="AzureTableDataStoreQueryException"></exception>
        Task<IList<DataStoreEntity<TData>>> FindWithMetadataAsync(Expression<Func<TData, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null, int? limit = null);

        /// <summary>
        /// Finds entities using the provided query expression, with the additional <see cref="DateTimeOffset"/> parameter representing the entity Timestamp.
        /// <para>
        /// Returns a list of the <typeparamref name="TData"/> typed entities wrapped into
        /// <see cref="DataStoreEntity{TData}"/> instances that also contain the row's ETag and Timestamp.
        /// </para>
        /// </summary>
        /// <param name="queryExpression">
        /// The query to use in expression form. The operators ==, !=, &gt;, &gt;=, &lt;, &lt;= and ! are supported, as well as the binary &amp;&amp; and || and parenthesis.
        /// <para>
        /// Example: <c>(entity, timestamp) => (entity.Category == "Worker" &amp;&amp; entity.Money &gt; 9000) || timestamp &lt; yesterdaysDate</c>
        /// </para>
        /// </param>
        /// <param name="selectExpression">
        /// The properties to get and return, as an expression. If null, populates all object properties.
        /// <para>
        /// Example: <c>entity => new { entity.UserId, entity.EmployeeType, entity.Contact.Email }</c>
        /// </para>
        /// </param>
        /// <param name="limit">The maximum number of limits to return. If null, does not limit the number of results.</param>
        /// <returns></returns>
        /// <exception cref="AzureTableDataStoreQueryException"></exception>
        Task<IList<DataStoreEntity<TData>>> FindWithMetadataAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null, int? limit = null);

        /// <summary>
        /// Gets a single entity matching the query expression, or null if no matching entity was found.
        /// </summary>
        /// <param name="queryExpression">
        /// The query to use in expression form. The operators ==, !=, &gt;, &gt;=, &lt;, &lt;= and ! are supported, as well as the binary &amp;&amp; and || and parenthesis.
        /// <para>
        /// Example: <c>entity => entity.Category == "Worker" &amp;&amp; entity.Money &gt; 9000</c>
        /// </para>
        /// </param>
        /// <param name="selectExpression">
        /// The properties to get and return, as an expression. If null, populates all object properties.
        /// <para>
        /// Example: <c>entity => new { entity.UserId, entity.EmployeeType, entity.Contact.Email }</c>
        /// </para>
        /// </param>
        /// <returns></returns>
        /// <exception cref="AzureTableDataStoreQueryException"></exception>
        Task<TData> GetAsync(Expression<Func<TData, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null);

        /// <summary>
        /// Gets a single entity matching the query expression, with the additional <see cref="DateTimeOffset"/> expression parameter representing the entity Timestamp.
        /// Returns null if no matching entity was found.
        /// </summary>
        /// <param name="queryExpression">
        /// The query to use in expression form. The operators ==, !=, &gt;, &gt;=, &lt;, &lt;= and ! are supported, as well as the binary &amp;&amp; and || and parenthesis.
        /// <para>
        /// Example: <c>entity => entity.Category == "Worker" &amp;&amp; entity.Money &gt; 9000</c>
        /// </para>
        /// </param>
        /// <param name="selectExpression">
        /// The properties to get and return, as an expression. If null, populates all object properties.
        /// <para>
        /// Example: <c>(entity, timestamp) => (entity.Category == "Worker" &amp;&amp; entity.Money &gt; 9000) || timestamp &lt; yesterdaysDate</c>
        /// </para>
        /// </param>
        /// <returns></returns>
        /// <exception cref="AzureTableDataStoreQueryException"></exception>
        Task<TData> GetAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null);

        /// <summary>
        /// Gets a single entity matching the query expression, or null if no matching entity was found.
        /// <para>
        /// Returns a <typeparamref name="TData"/> typed entity wrapped into
        /// <see cref="DataStoreEntity{TData}"/> instance that also contains the row's ETag and Timestamp.
        /// </para>
        /// </summary>
        /// <param name="queryExpression">
        /// The query to use in expression form. The operators ==, !=, &gt;, &gt;=, &lt;, &lt;= and ! are supported, as well as the binary &amp;&amp; and || and parenthesis.
        /// <para>
        /// Example: <c>entity => entity.Category == "Worker" &amp;&amp; entity.Money &gt; 9000</c>
        /// </para>
        /// </param>
        /// <param name="selectExpression">
        /// The properties to get and return, as an expression. If null, populates all object properties.
        /// <para>
        /// Example: <c>entity => new { entity.UserId, entity.EmployeeType, entity.Contact.Email }</c>
        /// </para>
        /// </param>
        /// <returns></returns>
        /// <exception cref="AzureTableDataStoreQueryException"></exception>
        Task<DataStoreEntity<TData>> GetWithMetadataAsync(Expression<Func<TData, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null);

        /// <summary>
        /// Gets a single entity matching the query expression, with the additional <see cref="DateTimeOffset"/> expression parameter representing the entity Timestamp.
        /// Returns null if no matching entity was found.
        /// <para>
        /// Returns a <typeparamref name="TData"/> typed entity wrapped into
        /// <see cref="DataStoreEntity{TData}"/> instance that also contains the row's ETag and Timestamp.
        /// </para>
        /// </summary>
        /// <param name="queryExpression">
        /// The query to use in expression form. The operators ==, !=, &gt;, &gt;=, &lt;, &lt;= and ! are supported, as well as the binary &amp;&amp; and || and parenthesis.
        /// <para>
        /// Example: <c>entity => entity.Category == "Worker" &amp;&amp; entity.Money &gt; 9000</c>
        /// </para>
        /// </param>
        /// <param name="selectExpression">
        /// The properties to get and return, as an expression. If null, populates all object properties.
        /// <para>
        /// Example: <c>entity => new { entity.UserId, entity.EmployeeType, entity.Contact.Email }</c>
        /// </para>
        /// </param>
        /// <returns></returns>
        /// <exception cref="AzureTableDataStoreQueryException"></exception>
        Task<DataStoreEntity<TData>> GetWithMetadataAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null);

        /// <summary>
        /// Deletes the entire source table, effectively deleting all its contents, as well as the entire blob container used to store the files.
        /// <para>
        /// Note: this action is irreversible.
        /// </para>
        /// </summary>
        /// <returns></returns>
        Task DeleteTableAndBlobContainerAsync();

        /// <summary>
        /// Deletes entities from table. If these entities also contain <see cref="LargeBlob"/> properties, those blobs will be deleted.
        /// <para>
        /// Note: this action is irreversible.
        /// </para>
        /// </summary>
        /// <param name="useBatching">
        /// Use batches to perform the deletion.
        /// <para>
        /// Batching cannot be used for deleting entities with <see cref="LargeBlob"/> properties as that cannot be done as a transaction.
        /// </para>
        /// </param>
        /// <param name="entities">The entities to delete.</param>
        /// <returns></returns>
        //Task DeleteAsync(bool useBatching, params TData[] entities);

        /// <summary>
        /// Deletes entities from table using the specified row and partition keys. If these entities also contain <see cref="LargeBlob"/> properties, those blobs will be deleted.
        /// <para>
        /// Note: this action is irreversible.
        /// </para>
        /// </summary>
        /// <param name="useBatching">
        /// Use batches to perform the deletion.
        /// <para>
        /// Batching cannot be used for deleting entities with <see cref="LargeBlob"/> properties as that cannot be done as a transaction.
        /// </para>
        /// </param>
        /// <param name="ids">The entity partition key + row key pairs to delete.</param>
        /// <returns></returns>
        //Task DeleteAsync(bool useBatching, params (string rowKey, string partitionKey)[] ids);

        /// <summary>
        /// Deletes entities from table that match the query expression. If these entities also contain <see cref="LargeBlob"/> properties, those blobs will be deleted.
        /// <para>
        /// Effectively performs a query, and then runs batch delete operations on those results.
        /// </para>
        /// <para>
        /// Note: this action is irreversible.
        /// </para>
        /// </summary>
        /// <param name="queryExpression">The query expression used to find entities to delete</param>
        /// <returns></returns>
        //Task DeleteAsync(Expression<Func<TData, bool>> queryExpression);
        //Task EnumerateAsync(Func<TData, Task> enumeratorFunc);
        //Task EnumerateAsync(Expression<Func<TData, bool>> queryExpression, Func<TData, Task> enumeratorFunc);
    }
}