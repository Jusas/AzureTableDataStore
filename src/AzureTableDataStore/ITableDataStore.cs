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
        /// <param name="entries">One ore more entries to insert of the <see cref="TData"/> type.</param>
        /// <returns></returns>
        Task InsertAsync(bool useBatching, params TData[] entries);
        
        /// <summary>
        /// Inserts or updates entities into Table Storage. If an entity with the same partition and row keys already exist,
        /// it will be replaced.
        /// </summary>
        /// <param name="useBatching"></param>
        /// <param name="entries"></param>
        /// <returns></returns>
        Task UpsertAsync(bool useBatching, params TData[] entries);

        /// <summary>
        /// Merges (aka updates) the property values from the provided entities using the properties
        /// selected in the expression.
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
        /// <param name="entries">The entries to update.</param>
        /// <returns></returns>
        Task MergeAsync(bool useBatching, Expression<Func<TData, object>> selectMergedPropertiesExpression,
            params TData[] entries);

        Task<IList<TData>> FindAsync(Expression<Func<TData, bool>> queryExpression);
        Task<IList<TData>> FindAsync(string query);
        Task<TData> GetAsync(Expression<Func<TData, bool>> queryExpression);
        Task<TData> GetAsync(string query);
        Task DeleteAsync(params TData[] entries);
        Task DeleteAsync(params string[] ids);
        Task DeleteAsync(Expression<Func<TData, bool>> queryExpression);
        Task DeleteAsync(string query);
        Task EnumerateAsync(Func<TData, Task> enumeratorFunc);
        Task EnumerateAsync(Expression<Func<TData, bool>> queryExpression, Func<TData, Task> enumeratorFunc);
        Task EnumerateAsync(string query, Func<TData, Task> enumeratorFunc);
    }
}