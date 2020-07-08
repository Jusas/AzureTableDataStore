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
    /// Interface for the <see cref="TableDataStore{TData}"/>.
    /// </summary>
    /// <typeparam name="TData">The entity type that is stored in the Azure Storage Table.</typeparam>
    public interface ITableDataStore<TData> : INamedTableDataStore where TData:new()
    {
        /// <summary>
        /// Inserts new entities into Table Storage.
        /// </summary>
        /// <param name="useBatching">
        /// Enables batch inserts, which are faster when inserting multiple entities.
        /// <para>Batch inserts are not available for entities with <see cref="LargeBlob"/> properties.</para>
        /// </param>
        /// <param name="entities">One ore more entities to insert of the <see cref="TData"/> type.</param>
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
        /// <exception cref="AzureTableDataStoreSingleOperationException"></exception>
        /// <exception cref="AzureTableDataStoreBatchedOperationException"></exception>
        /// <returns></returns>
        Task InsertAsync(bool useBatching, params TData[] entities);

        /// <summary>
        /// Inserts or replaces entities into Table Storage. When an entity with the same partition and row keys already exist,
        /// it will be replaced.
        /// </summary>
        /// <param name="useBatching">
        /// Enables batch inserts/replaces, which are faster with multiple entities.
        /// <para>Batching is not available for entity types with <see cref="LargeBlob"/> properties.</para>
        /// </param>
        /// <param name="entities">One or more entities to insert/replace</param>
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
        /// <exception cref="AzureTableDataStoreSingleOperationException"></exception>
        /// <exception cref="AzureTableDataStoreBatchedOperationException"></exception>
        /// <returns></returns>
        Task InsertOrReplaceAsync(bool useBatching, params TData[] entities);

        /// <summary>
        /// Merges (aka updates) the property values from the provided entities using the properties
        /// selected in the expression.
        /// <para>
        /// NOTE: this method assumes ETag: '*' for all entities, and therefore will overwrite indiscriminately.
        /// </para>
        /// <para>
        /// To use ETags, use <see cref="MergeAsync(bool,System.Linq.Expressions.Expression{System.Func{TData,object}},DataStoreEntity{TData}[])"/>
        /// </para>
        /// </summary>
        /// <param name="useBatching">Use batches to update the entity data. Gives better performance when there are
        /// a lot of entities to update.
        /// <para>
        /// Batching cannot be used for updating <see cref="LargeBlob"/> properties as that cannot be done as a transaction.
        /// </para>
        /// </param>
        /// <param name="selectMergedPropertiesExpression">
        /// An expression to select the properties to merge.
        /// <para>Example: entity => new { entity.Name, entity.Email }</para>
        /// </param>
        /// <param name="entities">The entities to update.</param>
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
        /// <exception cref="AzureTableDataStoreSingleOperationException"></exception>
        /// <exception cref="AzureTableDataStoreBatchedOperationException"></exception>
        /// <returns></returns>
        Task MergeAsync(bool useBatching, Expression<Func<TData, object>> selectMergedPropertiesExpression,
            params TData[] entities);

        /// <summary>
        /// Merges (aka updates) the property values from the provided entities using the properties
        /// selected in the expression.
        /// <para>
        /// Uses optimistic concurrency, i.e. the <see cref="DataStoreEntity{TData}.ETag"/> matching determines whether or not
        /// the merge can be made.
        /// </para>
        /// </summary>
        /// <param name="useBatching">Use batches to update the entity data. Gives better performance when there are
        /// a lot of entities to update.
        /// <para>
        /// Batching cannot be used for updating <see cref="LargeBlob"/> properties as that cannot be done as a transaction. However if the
        /// LargeBlob properties are not selected for the merge, batching can be used.
        /// </para>
        /// </param>
        /// <param name="selectMergedPropertiesExpression">
        /// An expression to select the properties to merge.
        /// <para>
        /// Example: <c>entity => new { entity.UserId, entity.EmployeeType, entity.Contact.Email }</c>
        /// </para>
        /// </param>
        /// <param name="entities">The entities to update, wrapped into <see cref="DataStoreEntity{TData}"/> objects to provide ETags.</param>
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
        /// <exception cref="AzureTableDataStoreSingleOperationException"></exception>
        /// <exception cref="AzureTableDataStoreBatchedOperationException"></exception>
        /// <returns></returns>
        Task MergeAsync(bool useBatching, Expression<Func<TData, object>> selectMergedPropertiesExpression,
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
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
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
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
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
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
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
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
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
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
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
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
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
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
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
        /// <exception cref="AzureTableDataStoreInternalException"></exception>
        /// <exception cref="AzureTableDataStoreQueryException"></exception>
        Task<DataStoreEntity<TData>> GetWithMetadataAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression, Expression<Func<TData, object>> selectExpression = null);

        //Task DeleteAsync(params TData[] entities);
        //Task DeleteAsync(params string[] ids);
        //Task DeleteAsync(Expression<Func<TData, bool>> queryExpression);
        //Task EnumerateAsync(Func<TData, Task> enumeratorFunc);
        //Task EnumerateAsync(Expression<Func<TData, bool>> queryExpression, Func<TData, Task> enumeratorFunc);
    }
}