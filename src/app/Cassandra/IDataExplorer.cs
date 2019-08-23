using System;
using System.Collections.Generic;
using automation.core.components.data.v1.Entities;

namespace automation.core.components.data.v1.Cassandra
{
    public interface IDataExplorer<T>
    {
        bool Add(string key, T value);
        bool Add(string key, T value, uint timeToLive);
        bool Add(DataExplorer<T>.Data data);
        bool Add(DataExplorer<T>.Data data, uint timeToLive);
        bool Add(List<DataExplorer<T>.Data> data);
        bool Add(List<DataExplorer<T>.Data> data, uint timeToLive);

        T Get(string key);
        T Get(string key, bool executeInSingleNode);
        List<T> Get(List<string> keys);
        List<T> Get(List<string> keys, bool executeInSingleNode);
        List<string> GetKeys();
        List<T> Get(int limit);
        string GetAsString(string key);
        string GetAsString(string key, bool executeInSingleNode);
        Dictionary<string, string> GetAsString(string columnFamily, int limit);
        Dictionary<string, string> GetAsString(string columnFamily, int limit, bool executeInSingleNode);
        Dictionary<string, string> GetAsString(List<string> keys);
        Dictionary<string, string> GetAsString(List<string> keys, bool executeInSingleNode);
        Dictionary<string, string> GetAsString(string columnFamily, List<string> keys);
        Dictionary<string, string> GetAsString(string columnFamily, List<string> keys, bool executeInSingleNode);
        T Get(string columnFamily, string key);
        List<T> Get(string columnFamily, List<string> keys);

        // Deprecated Methods
        T Get(string key, int retryCount, bool resetDBConnection);
        T Get(string columnFamily, string key, int retryCount, bool resetDBConnection);
        List<T> Get(List<string> keys, int retryCount, bool resetDBConnection);
        List<T> Get(string columnFamily, List<string> keys, int retryCount, bool resetDBConnection);
        // End Deprecated Methods

        List<string> Filter(string columnName, DateTime to);
        List<string> Filter(string columnName, DateTime from, DateTime to);
        List<string> Filter(string columnName, DateTime to, int limit);
        List<string> Filter(string columnName, DateTime from, DateTime to, int limit);

        bool Delete(string key);

        bool AddIndex(string columnFamily, Dictionary<string, string> keyValues);
        bool AddIndex(string columnFamily, Dictionary<string, string> keyValues, uint timeToLive);
        bool AddIndex(string columnFamily, string key, string indexKey, string indexValue);
        bool AddIndex(string columnFamily, string key, string indexKey, string indexValue, uint timeToLive);
        bool AddIndexes(string columnFamily, Index index);
        bool AddIndexes(string columnFamily, Index index, uint timeToLive);
        bool AddIndexes(string columnFamily, List<Index> indexes);
        bool AddIndexes(string columnFamily, List<Index> indexes, uint timeToLive);

        // Depricated Method
        bool AddIndexes(string columnFamily, List<Index> indexes, uint timeToLive, int retryCount = 0);

        bool AddIndexes(string columnFamily, TimeIndex index);
        bool AddIndexes(string columnFamily, TimeIndex index, uint timeToLive);
        bool AddIndexes(string columnFamily, List<TimeIndex> indexes);
        bool AddIndexes(string columnFamily, List<TimeIndex> indexes, uint timeToLive);

        bool AddIndexes(string columnFamily, TimeUUIDIndex index);
        bool AddIndexes(string columnFamily, TimeUUIDIndex index, uint timeToLive);
        bool AddIndexes(string columnFamily, List<TimeUUIDIndex> indexes);
        bool AddIndexes(string columnFamily, List<TimeUUIDIndex> indexes, uint timeToLive);

        bool AddIndexWithTimeUUID(string columnFamily, string key, string indexKey, string indexValue);
        bool AddIndexWithTimeUUID(string columnFamily, string key, string indexKey, string indexValue, uint timeToLive);
        bool AddIndexesWithTimeUUID(string columnFamily, List<Index> indexes);
        bool AddIndexesWithTimeUUID(string columnFamily, List<Index> indexes, uint timeToLive);
        [Obsolete("duplicate of Get index method")]
        Dictionary<string, string> GetUniqueIndex(string columnFamily, string key);
        [Obsolete("duplicate of Get index method")]
        Dictionary<string, string> GetUniqueIndex(string columnFamily, string key, int limit);
        [Obsolete("duplicate of Get index method")]
        Dictionary<string, string> GetUniqueIndex(string columnFamily, string key, string indexKey, int limit);

        Dictionary<string, string> GetIndex(string columnFamily, string key);
        Dictionary<string, string> GetIndex(string columnFamily, string key, int limit);
        List<string> GetIndexColumnNames(string columnFamily, string key);
        List<string> GetIndexColumnNames(string columnFamily, string key, int limit);
        List<string> GetIndexColumnValues(string columnFamily, string key);
        List<string> GetIndexColumnValues(string columnFamily, string key, int limit);


        Dictionary<string, string> GetIndex(string columnFamily, string key, string indexKey);
        Dictionary<string, string> GetIndex(string columnFamily, string key, string indexKey, int limit);
        List<string> GetIndexColumnNames(string columnFamily, string key, string indexKey);
        List<string> GetIndexColumnNames(string columnFamily, string key, string indexKey, int limit);
        List<string> GetIndexColumnValues(string columnFamily, string key, string indexKey);
        List<string> GetIndexColumnValues(string columnFamily, string key, string indexKey, int limit);

        /// <summary>
        /// Returns requests columns values list
        /// </summary>
        /// <param name="columnFamily"></param>
        /// <param name="requestColumn"></param>
        /// <param name="whereClause"></param>
        /// <param name="limit"></param>
        /// <param name="executeWithCLOne"></param>
        /// <param name="executeWithCLLocalQuorum"></param>
        /// <returns></returns>
        List<string> GetColumnValues(string columnFamily, string requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne,
            bool executeWithCLLocalQuorum);
        Dictionary<string, string> GetIndexesColumns(string columnFamily);
        Dictionary<string, string> GetIndexesColumns(string columnFamily, int limit);
        List<Index> GetIndexes(string columnFamily, int limit);
        List<Index> GetIndexes(string columnFamily, List<string> keys);

        Dictionary<string, string> GetTimeIndex(string columnFamily, string key, DateTime from, DateTime to);
        Dictionary<string, string> GetTimeIndex(string columnFamily, string key, DateTime from, DateTime to, int limit);
        Dictionary<string, string> GetTimeIndex(string columnFamily, string key, string from, string to);
        Dictionary<string, string> GetTimeIndex(string columnFamily, string key, string from, string to, int limit);
        Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to);
        Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to, int limit);
        Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending);

        List<string> GetTimeIndexColumnNames(string columnFamily, string key, DateTime from, DateTime to);
        List<string> GetTimeIndexColumnNames(string columnFamily, string key, DateTime from, DateTime to, int limit);
        List<string> GetTimeIndexColumnNames(string columnFamily, string key, string from, string to);
        List<string> GetTimeIndexColumnNames(string columnFamily, string key, string from, string to, int limit);
        List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, DateTime from, DateTime to);
        List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, DateTime from, DateTime to, int limit);
        List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to);
        List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to, int limit);
        List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending);

        List<string> GetTimeIndexColumnValues(string columnFamily, string key, DateTime from, DateTime to);
        List<string> GetTimeIndexColumnValues(string columnFamily, string key, DateTime from, DateTime to, int limit);
        List<string> GetTimeIndexColumnValues(string columnFamily, string key, string from, string to);
        List<string> GetTimeIndexColumnValues(string columnFamily, string key, string from, string to, int limit);
        List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, DateTime from, DateTime to);
        List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, DateTime from, DateTime to, int limit);
        List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to);
        List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to, int limit);
        List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending);
        List<string> GetTimeIndexsColumnValues(string columnFamily, string columnName, string keys, string from, string to, int limit, bool ascending);
        List<string> GetTimeIndexsColumnValues(string columnFamily, string columnName, List<string> keys, string from, string to, int limit, bool ascending);

        List<T> GetAllRows(string columnFamily, int limit);

        bool DeleteIndex(string columnFamily, string key);
        bool DeleteIndex(string columnFamily, string key, string indexKey);
        bool DeleteIndex(string columnFamily, string columnName, string key, string indexKey);

        bool DeleteTimeIndex(string columnFamily, string key, string indexKey);
        bool DeleteTimeIndex(string columnFamily, string columnName, string key, string indexKey);

        int Count(string columnFamily, string key);
        int Count(string columnFamily, string key, int limit);

        bool ConnectivityCheck();

        Guid GetTimeBasedGuid(DateTime dt);
        DateTime GetDateFromGuid(Guid guid);
        [Obsolete("use other ExecuteQuery method with request columns as parameters")]
        List<T> ExecuteQuery<T>(string query);

        List<T> ExecuteQuery<T>(string columnFamily, List<string> requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne = false,
            bool executeWithCLLocalQuorum = false);

        bool ExecuteNonQuery(string columnFamily, List<RowIndex> indexes);
        bool ExecuteNonQuery(string columnFamily, List<RowIndex> indexes, uint timeToLive);

        Dictionary<string, object> GetDataAsObject(string columnFamily, List<string> keys, string valueColumn);
        Dictionary<string, object> GetDataAsObject(string columnFamily, List<string> keys, string valueColumn, string keyColumn);
        bool UpsertCounter(string columnFamily, List<CounterIndex> indexes);
        List<T> GetIndexes(string columnFamily, Dictionary<string, string> whereColumnValues);
        List<T> GetIndexes(string columnFamily, Dictionary<string, string> whereColumnValues, int limit);
        List<T> GetIndexes(string columnFamily, List<string> returnColumn, Dictionary<string, string> whereColumnValues);
        List<T> GetIndexes(string columnFamily, List<string> returnColumn, Dictionary<string, string> whereColumnValues, int limit);

        List<T> GetIndexesAsObject(string columnFamily, List<string> returnColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne,
            bool executeWithCLLocalQuorum);
        bool DeleteIndex(string columnFamily, Dictionary<string, object> whereColumnValues);

        /// <summary>
        /// Operation to update column values for the specified column family
        /// </summary>
        /// <param name="columnFamily"> The column family name</param>
        /// <param name="whereColumnValues">The where column values</param>
        /// <param name="setColumnValues">The set column values</param>
        /// <param name="autoTimeUUIDValues">The flags to update the timestamp or not,
        /// currently modification_timestamp column is supported, if the column is available for the column 
        /// family pass key as EnableAutoTimeUUID and value as true or false</param>
        /// <returns>Returns boolean value</returns>
        bool Update(string columnFamily, Dictionary<string, object> whereColumnValues, Dictionary<string, object> setColumnValues, Dictionary<string, bool> autoTimeUUIDValues);
    }
}
