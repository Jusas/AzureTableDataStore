using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace AzureTableDataStore
{
    public interface ITableDataStore
    {
        string Name { get; }
    }
    // todo: include entity metadata in responses to get ETags and Timestamps?
    public interface ITableDataStore<TData> : ITableDataStore where TData:new()
    {
        /// <summary>
        /// Inserts new entities into Table Storage.
        /// </summary>
        /// <param name="useBatching">
        /// Enables batch inserts, which are faster when inserting multiple entities.
        /// <para>Batch inserts are not available for entities with <see cref="LargeBlob"/> properties.</para>
        /// </param>
        /// <param name="entities">One ore more entities to insert of the <see cref="TData"/> type.</param>
        /// <exception cref="AzureTableDataStoreException"></exception>
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
        /// <exception cref="AzureTableDataStoreException"></exception>
        /// <returns></returns>
        Task InsertOrReplaceAsync(bool useBatching, params TData[] entities);

        /// <summary>
        /// Merges (aka updates) the property values from the provided entities using the properties
        /// selected in the expression.
        /// <para>
        /// Note: this method assumes ETag: '*' for all entities, and therefore will overwrite indiscriminately.
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
        /// <exception cref="AzureTableDataStoreException"></exception>
        /// <returns></returns>
        Task MergeAsync(bool useBatching, Expression<Func<TData, object>> selectMergedPropertiesExpression,
            params TData[] entities);

        /// <summary>
        /// Merges (aka updates) the property values from the provided entities using the properties
        /// selected in the expression.
        /// <para>
        /// Uses optimistic concurrency using <see cref="DataStoreEntity{TData}.ETag"/> to determine whether or not
        /// the merge can be made.
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
        /// <param name="entities">The entities to update, wrapped into <see cref="DataStoreEntity{TData}"/> objects to provide ETags.</param>
        /// <exception cref="AzureTableDataStoreException"></exception>
        /// <returns></returns>
        Task MergeAsync(bool useBatching, Expression<Func<TData, object>> selectMergedPropertiesExpression,
            params DataStoreEntity<TData>[] entities);

        //Task<IList<TData>> FindAsync(Expression<Func<TData, bool>> queryExpression);
        //Task<IList<TData>> FindAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression);

        //Task<IList<DataStoreEntity<TData>>> FindWithMetadataAsync(Expression<Func<TData, bool>> queryExpression);
        //Task<IList<DataStoreEntity<TData>>> FindWithMetadataAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression);



        Task<TData> GetAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression);
        Task<TData> GetAsync(Expression<Func<TData, bool>> queryExpression);


        Task<DataStoreEntity<TData>> GetWithMetadataAsync(Expression<Func<TData, bool>> queryExpression);
        Task<DataStoreEntity<TData>> GetWithMetadataAsync(Expression<Func<TData, DateTimeOffset, bool>> queryExpression);

        //Task DeleteAsync(params TData[] entities);
        //Task DeleteAsync(params string[] ids);
        //Task DeleteAsync(Expression<Func<TData, bool>> queryExpression);
        //Task EnumerateAsync(Func<TData, Task> enumeratorFunc);
        //Task EnumerateAsync(Expression<Func<TData, bool>> queryExpression, Func<TData, Task> enumeratorFunc);
    }
}