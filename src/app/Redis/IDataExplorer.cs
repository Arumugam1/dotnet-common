using System;
using System.Collections.Generic;
using StackExchange.Redis;

namespace automation.core.components.data.v1.Redis
{
    public interface IDataExplorer<T>
    {
        bool Add(string key, string value);

        bool Add(string key, string value, uint timeToLive);

        bool Add(string key, T value);

        bool Add(string key, T value, uint timeToLive);

        bool Add(DataExplorer<T>.Data data);

        bool Add(DataExplorer<T>.Data data, uint timeToLive);

        bool Add(List<DataExplorer<T>.Data> data);

        bool Add(List<DataExplorer<T>.Data> data, uint timeToLive);

        T Get(string key);

        string GetValue(string key);

        string GetValue(string key, CommandFlags flag);

        List<T> Get(List<string> keys);

        List<T> Get(List<string> keys, CommandFlags flag);

        string GetAsString(string key);

        Dictionary<string, string> GetAsString(List<string> keys);

        Dictionary<string, string> GetAsString(List<string> keys, CommandFlags flag);

        bool Delete(string key);

        bool AddIndex(string key, string indexKey, string indexValue);

        bool AddIndex(string key, string indexKey, string indexValue, uint timeToLive);

        bool AddIndexes(Index index);

        bool AddIndexes(Index index, uint timeToLive);

        bool AddIndexes(List<Index> indexes);

        bool AddIndexes(List<Index> indexes, uint timeToLive);

        Dictionary<string, string> GetIndex(string key);

        Dictionary<string, string> GetIndex(string key, int limit);

        List<string> GetIndexColumnNames(string key);

        List<string> GetIndexColumnNames(string key, int limit);

        List<string> GetIndexColumnValues(string key);

        List<string> GetIndexColumnValues(string key, int limit);

        string GetIndexValue(string key, string indexKey);

        Dictionary<string, string> GetIndex(string key, int limit, CommandFlags flag);

        List<string> GetIndexColumnNames(string key, int limit, CommandFlags flag);

        List<string> GetIndexColumnValues(string key, int limit, CommandFlags flag);

        string GetIndexValue(string key, string indexKey, CommandFlags flag);

        bool DeleteIndex(string key);

        bool DeleteIndex(string key, string indexKey);

        bool DeleteIndex(string columnName, string key, string indexKey);

        int Count(string key);

        int Count(string key, int limit);

        int Count(string key, int limit, CommandFlags flag);

        Guid GetTimeBasedGuid(DateTime dt);

        bool ConnectivityCheck();
    }
}