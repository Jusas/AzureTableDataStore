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

    public interface ITableDataStore<TData> : ITableDataStore
    {
        Task InsertAsync(params TData[] entries);
        Task UpsertAsync(params TData[] entries);
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