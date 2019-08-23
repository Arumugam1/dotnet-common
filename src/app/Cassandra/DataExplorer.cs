using System;
using System.Collections.Generic;
using automation.components.data.v1.Buffer;
using automation.components.data.v1.Entities;

namespace automation.components.data.v1.Cassandra
{
    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class DataExplorer<T> : IDataExplorer<T>, IDisposable
    {
        private const string DeprecationMessage = "[ARIC-6571] Retries are now handled via RetryPolicy";

        /// <summary>
        /// Gets or sets gets or Sets Key Space
        /// </summary>
        internal string KeySpace { get; set; }

        /// <summary>
        /// Gets or sets gets or Sets  Column Family
        /// </summary>
        internal string ColumnFamily { get; set; }

        /// <summary>
        /// Gets or sets gets or Sets  Key Prefix
        /// </summary>
        internal string KeyPrefix { get; set; }

        protected static Dictionary<string, Entity<T>> bufferDB = new Dictionary<string, Entity<T>>();
        protected static object buffer_lock = new object();
        private ICassandraDataExplorer CassandraDataExplorer { get; set; }

        protected DataExplorer(string keyspace, string columnFamily, string keyPrefix = "")
        {
            this.CassandraDataExplorer = Container.Resolve<ICassandraDataExplorer>();

            if (this.CassandraDataExplorer == null)
            {
                this.CassandraDataExplorer = new DSEDataExplorer(keyspace, columnFamily, keyPrefix);
            }

            this.KeySpace = keyspace;
            this.ColumnFamily = columnFamily;
            this.KeyPrefix = keyPrefix;
        }

        public virtual List<T> GetIndexes(string columnFamily, Dictionary<string, string> whereColumnValues)
        {
            return this.CassandraDataExplorer.GetIndexes<T>(columnFamily, new List<string>(), whereColumnValues, 0);
        }

        public virtual List<T> GetIndexes(string columnFamily, Dictionary<string, string> whereColumnValues, int limit)
        {
            return this.CassandraDataExplorer.GetIndexes<T>(columnFamily, new List<string>(), whereColumnValues, limit);
        }

        public virtual List<T> GetIndexes(string columnFamily, List<string> returnColumn, Dictionary<string, string> whereColumnValues)
        {
            return this.CassandraDataExplorer.GetIndexes<T>(columnFamily, returnColumn, whereColumnValues, 0);
        }

        public virtual List<T> GetIndexes(string columnFamily, List<string> returnColumn, Dictionary<string, string> whereColumnValues, int limit)
        {
            return this.CassandraDataExplorer.GetIndexes<T>(columnFamily, returnColumn, whereColumnValues, limit);
        }

        public virtual List<T> GetIndexesAsObject(string columnFamily, List<string> returnColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne, bool executeWithCLLocalQuorum)
        {
            return this.CassandraDataExplorer.GetIndexesAsObject<T>(columnFamily, returnColumn, whereClause, limit, executeWithCLOne, executeWithCLLocalQuorum);
        }

        public virtual bool DeleteIndex(string columnFamily, Dictionary<string, object> whereColumnValues)
        {
            return this.CassandraDataExplorer.DeleteIndex(columnFamily, whereColumnValues);
        }

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
        public virtual bool Update(string columnFamily, Dictionary<string, object> whereColumnValues, Dictionary<string, object> setColumnValues,
            Dictionary<string,bool> autoTimeUUIDValues)
        {
            return this.CassandraDataExplorer.Update(columnFamily, whereColumnValues, setColumnValues, autoTimeUUIDValues);
        }

        public virtual bool UpsertCounter(string columnFamily, List<CounterIndex> indexes)
        {
            return this.CassandraDataExplorer.UpsertCounter(columnFamily, indexes);
        }

        public virtual Dictionary<string, object> GetDataAsObject(string columnFamily, List<string> keys, string valueColumn)
        {
            return this.CassandraDataExplorer.GetDataAsObject(columnFamily, keys, valueColumn);
        }

        public virtual Dictionary<string, object> GetDataAsObject(string columnFamily, List<string> keys, string valueColumn, string keyColumn)
        {
            return this.CassandraDataExplorer.GetDataAsObject(columnFamily, keys, valueColumn, keyColumn);
        }

        public virtual bool Add(string key, T value)
        {
            return this.CassandraDataExplorer.Add(new List<Data> { new Data { Key = key, Value = value } }, 0);
        }

        public virtual bool Add(string key, T value, uint timeToLive)
        {
            return this.CassandraDataExplorer.Add(new List<Data> { new Data { Key = key, Value = value } }, timeToLive);
        }

        public virtual bool Add(Data data)
        {
            return this.CassandraDataExplorer.Add(new List<Data> { data }, 0);
        }

        public virtual bool Add(Data data, uint timeToLive)
        {
            return this.CassandraDataExplorer.Add(new List<Data> { data }, timeToLive);
        }

        public virtual bool Add(List<Data> data)
        {
            return this.CassandraDataExplorer.Add(data, 0);
        }

        public virtual bool ConnectivityCheck()
        {
            return this.CassandraDataExplorer.ConnectivityCheck();
        }

        public virtual bool Add(List<Data> data, uint timeToLive)
        {
            return this.CassandraDataExplorer.Add(data, timeToLive);
        }

        public virtual T Get(string key)
        {
            return this.CassandraDataExplorer.Get<T>(key);
        }

        /// <summary>
        /// This method is more expensive and execute a query in single node
        /// </summary>
        /// <param name="key"></param>
        /// <param name="executeInSingleNode"></param>
        /// <returns></returns>
        public virtual T Get(string key, bool executeInSingleNode)
        {
            return this.CassandraDataExplorer.Get<T>(key, executeInSingleNode);
        }

        public virtual List<T> Get(int limit)
        {
            return this.CassandraDataExplorer.Get<T>(limit);
        }

        public virtual List<string> GetKeys()
        {
            return this.CassandraDataExplorer.GetKeys();
        }

        public virtual List<T> Get(List<string> keys)
        {
            return this.CassandraDataExplorer.Get<T>(keys);
        }

        public virtual List<T> Get(List<string> keys, bool executeInSingleNode)
        {
            return this.CassandraDataExplorer.Get<T>(keys, executeInSingleNode);
        }

        public virtual string GetAsString(string key)
        {
            return this.CassandraDataExplorer.GetAsString(key);
        }

        /// <summary>
        /// This method is more expensive and execute a query in single node
        /// </summary>
        /// <param name="key"></param>
        /// <param name="executeInSingleNode"></param>
        /// <returns></returns>
        public virtual string GetAsString(string key, bool executeInSingleNode)
        {
            return this.CassandraDataExplorer.GetAsString(key, executeInSingleNode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnFamily"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        public virtual Dictionary<string, string> GetAsString(string columnFamily, int limit)
        {
            return this.CassandraDataExplorer.GetAsString(columnFamily, limit);
        }

        /// <summary>
        /// This method is more expensive and execute a query in single node
        /// </summary>
        /// <param name="columnFamily"></param>
        /// <param name="limit"></param>
        /// <param name="executeInSingleNode"></param>
        /// <returns></returns>
        public virtual Dictionary<string, string> GetAsString(string columnFamily, int limit, bool executeInSingleNode)
        {
            return this.CassandraDataExplorer.GetAsString(columnFamily, limit, executeInSingleNode);
        }

        public virtual Dictionary<string, string> GetAsString(List<string> keys)
        {
            return this.CassandraDataExplorer.GetAsString(keys);
        }

        public virtual T Get(string columnFamily, string key)
        {
            return this.CassandraDataExplorer.Get<T>(columnFamily, key);
        }

        /// <summary>
        /// This method is more expensive and execute a query in single node
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="executeInSingleNode"></param>
        /// <returns></returns>
        public virtual Dictionary<string, string> GetAsString(List<string> keys, bool executeInSingleNode)
        {
            return this.CassandraDataExplorer.GetAsString(keys, executeInSingleNode);
        }

        public virtual Dictionary<string, string> GetAsString(string columnFamily, List<string> keys)
        {
            return this.CassandraDataExplorer.GetAsString(columnFamily, keys);
        }

        /// <summary>
        /// This method is more expensive and execute a query in single node
        /// </summary>
        /// <param name="columnFamily"></param>
        /// <param name="keys"></param>
        /// <param name="executeInSingleNode"></param>
        /// <returns></returns>
        public virtual Dictionary<string, string> GetAsString(string columnFamily, List<string> keys, bool executeInSingleNode)
        {
            return this.CassandraDataExplorer.GetAsString(columnFamily, keys, executeInSingleNode);
        }

        public virtual List<T> Get(string columnFamily, List<string> keys)
        {
            return this.CassandraDataExplorer.Get<T>(columnFamily, keys);
        }

        public virtual List<string> Filter(string columnName, DateTime to)
        {
            return this.CassandraDataExplorer.Filter(columnName, to);
        }

        public virtual List<string> Filter(string columnName, DateTime to, int limit)
        {
            return this.CassandraDataExplorer.Filter(columnName, to, limit);
        }

        public virtual List<string> Filter(string columnName, DateTime from, DateTime to)
        {
            return this.CassandraDataExplorer.Filter(columnName, from, to);
        }

        public virtual List<string> Filter(string columnName, DateTime from, DateTime to, int limit)
        {
            return this.CassandraDataExplorer.Filter(columnName, from, to, limit);
        }

        public virtual List<T> GetAllRows(string columnFamily, int limit)
        {
            return this.CassandraDataExplorer.GetAllRows<T>(columnFamily, limit);
        }

        public virtual List<T> GetAllRows(string columnFamily, List<string> requestColumn, int limit)
        {
            return this.CassandraDataExplorer.GetAllRows<T>(columnFamily, requestColumn, limit);
        }

        public virtual bool Delete(string key)
        {
            return this.CassandraDataExplorer.Delete(key);
        }

        public virtual bool AddIndex(string columnFamily, Dictionary<string, string> keyValues)
        {
            return this.CassandraDataExplorer.AddIndex(columnFamily, keyValues);
        }

        public virtual bool AddIndex(string columnFamily, Dictionary<string, string> keyValues, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndex(columnFamily, keyValues, timeToLive);
        }

        public virtual bool AddIndex(string columnFamily, string key, string indexKey, string indexValue)
        {
            return this.CassandraDataExplorer.AddIndex(columnFamily, key, indexKey, indexValue);
        }

        public virtual bool AddIndex(string columnFamily, string key, string indexKey, string indexValue, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndex(columnFamily, key, indexKey, indexValue, timeToLive);
        }

        public virtual bool AddIndexWithTimeUUID(string columnFamily, string key, string indexKey, string indexValue)
        {
            return this.CassandraDataExplorer.AddIndexWithTimeUUID(columnFamily, key, indexKey, indexValue);
        }

        public virtual bool AddIndexWithTimeUUID(string columnFamily, string key, string indexKey, string indexValue, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndexWithTimeUUID(columnFamily, key, indexKey, indexValue, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, Index index)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, index);
        }

        public virtual bool AddIndexes(string columnFamily, Index index, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, index, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, List<Index> indexes)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, indexes);
        }

        public virtual bool AddIndexes(string columnFamily, List<Index> indexes, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, indexes, 0);
        }

        public virtual bool AddIndexesWithTimeUUID(string columnFamily, List<Index> indexes)
        {
            return this.CassandraDataExplorer.AddIndexesWithTimeUUID(columnFamily, indexes);
        }

        public virtual bool AddIndexesWithTimeUUID(string columnFamily, List<Index> indexes, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndexesWithTimeUUID(columnFamily, indexes, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, TimeIndex index)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, index);
        }

        public virtual bool AddIndexes(string columnFamily, TimeIndex index, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, index, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, List<TimeIndex> indexes)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, indexes);
        }

        public virtual bool AddIndexes(string columnFamily, List<TimeIndex> indexes, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, indexes, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, TimeUUIDIndex index)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, new List<TimeUUIDIndex> { index }, 0);
        }

        public virtual bool AddIndexes(string columnFamily, TimeUUIDIndex index, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, new List<TimeUUIDIndex> { index }, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, List<TimeUUIDIndex> indexes)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, indexes, 0);
        }

        public virtual bool AddIndexes(string columnFamily, List<TimeUUIDIndex> indexes, uint timeToLive)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, indexes, timeToLive);
        }

        public virtual Dictionary<string, string> GetIndex(string columnFamily, string key)
        {
            return this.CassandraDataExplorer.GetIndex(columnFamily, key);
        }

        [Obsolete("duplicate of Get index method")]
        public virtual Dictionary<string, string> GetUniqueIndex(string columnFamily, string key)
        {
            return this.CassandraDataExplorer.GetUniqueIndex(columnFamily, key);
        }

        public virtual Dictionary<string, string> GetIndex(string columnFamily, string key, int limit)
        {
            return this.CassandraDataExplorer.GetIndex(columnFamily, key, limit);
        }

        [Obsolete("duplicate of Get index method")]
        public virtual Dictionary<string, string> GetUniqueIndex(string columnFamily, string key, int limit)
        {
            return this.CassandraDataExplorer.GetUniqueIndex(columnFamily, key, limit);
        }

        public virtual Dictionary<string, string> GetIndexesColumns(string columnFamily)
        {
            return this.CassandraDataExplorer.GetIndexesColumns(columnFamily);
        }

        public virtual Dictionary<string, string> GetIndexesColumns(string columnFamily, int limit)
        {
            return this.CassandraDataExplorer.GetIndexesColumns(columnFamily, limit);
        }

        public virtual List<Index> GetIndexes(string columnFamily, int limit)
        {
            return this.CassandraDataExplorer.GetIndexes(columnFamily, limit);
        }

        public virtual List<Index> GetIndexes(string columnFamily, List<string> keys)
        {
            return this.CassandraDataExplorer.GetIndexes(columnFamily, keys);
        }

        public virtual List<string> GetIndexColumnNames(string columnFamily, string key)
        {
            return this.CassandraDataExplorer.GetIndexColumnNames(columnFamily, key);
        }

        public virtual List<string> GetIndexColumnNames(string columnFamily, string key, int limit)
        {
            return this.CassandraDataExplorer.GetIndexColumnNames(columnFamily, key, limit);
        }

        public virtual List<string> GetIndexColumnValues(string columnFamily, string key)
        {
            return this.CassandraDataExplorer.GetIndexColumnValues(columnFamily, key);
        }

        public virtual List<string> GetIndexColumnValues(string columnFamily, string key, int limit)
        {
            return this.CassandraDataExplorer.GetIndexColumnValues(columnFamily, key, limit);
        }

        public virtual Dictionary<string, string> GetIndex(string columnFamily, string key, string indexKey)
        {
            return this.CassandraDataExplorer.GetIndex(columnFamily, key, indexKey);
        }

        public virtual Dictionary<string, string> GetIndex(string columnFamily, string key, string indexKey, int limit)
        {
            return this.CassandraDataExplorer.GetIndex(columnFamily, key, indexKey, limit);
        }

        [Obsolete("duplicate of Get index method")]
        public virtual Dictionary<string, string> GetUniqueIndex(string columnFamily, string key, string indexKey, int limit)
        {
            return this.CassandraDataExplorer.GetUniqueIndex(columnFamily, key, indexKey, limit);
        }

        public virtual List<string> GetIndexColumnNames(string columnFamily, string key, string indexKey)
        {
            return this.CassandraDataExplorer.GetIndexColumnNames(columnFamily, key, indexKey);
        }

        public virtual List<string> GetIndexColumnNames(string columnFamily, string key, string indexKey, int limit)
        {
            return this.CassandraDataExplorer.GetIndexColumnNames(columnFamily, key, indexKey, limit);
        }

        public virtual List<string> GetIndexColumnValues(string columnFamily, string key, string indexKey)
        {
            return this.CassandraDataExplorer.GetIndexColumnValues(columnFamily, key, indexKey);
        }

        public virtual List<string> GetIndexColumnValues(string columnFamily, string key, string indexKey, int limit)
        {
            return this.CassandraDataExplorer.GetIndexColumnValues(columnFamily, key, indexKey, limit);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string key, DateTime from, DateTime to)
        {
            return this.CassandraDataExplorer.GetTimeIndex(columnFamily, key, from, to);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string key, DateTime from, DateTime to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndex(columnFamily, key, from, to, limit);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string key, string from, string to)
        {
            return this.CassandraDataExplorer.GetTimeIndex(columnFamily, key, from, to);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string key, string from, string to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndex(columnFamily, key, from, to, limit);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to)
        {
            return this.CassandraDataExplorer.GetTimeIndex(columnFamily, columnName, key, from, to);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndex(columnFamily, columnName, key, from, to, limit);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending)
        {
            return this.CassandraDataExplorer.GetTimeIndex(columnFamily, columnName, key, from, to, limit, ascending);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string key, DateTime from, DateTime to)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnNames(columnFamily, key, from, to);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string key, DateTime from, DateTime to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnNames(columnFamily, key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string key, string from, string to)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnNames(columnFamily, key, from, to);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string key, string from, string to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnNames(columnFamily, key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, DateTime from, DateTime to)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnNames(columnFamily, columnName, key, from, to);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, DateTime from, DateTime to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnNames(columnFamily, columnName, key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnNames(columnFamily, columnName, key, from, to);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnNames(columnFamily, columnName, key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnNames(columnFamily, columnName, key, from, to, limit, ascending);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string key, DateTime from, DateTime to)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnValues(columnFamily, key, from, to);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string key, DateTime from, DateTime to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnValues(columnFamily, key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string key, string from, string to)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnValues(columnFamily, key, from, to);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string key, string from, string to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnValues(columnFamily, key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, DateTime from, DateTime to)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnValues(columnFamily, columnName, key, from, to);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, DateTime from, DateTime to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnValues(columnFamily, columnName, key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnValues(columnFamily, columnName, key, from, to);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to, int limit)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnValues(columnFamily, columnName, key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending)
        {
            return this.CassandraDataExplorer.GetTimeIndexColumnValues(columnFamily, columnName, key, from, to, limit, ascending);
        }

        [Obsolete("Use the overload where keys is a list of strings!")]
        public virtual List<string> GetTimeIndexsColumnValues(string columnFamily, string columnName, string keys, string from, string to, int limit, bool ascending)
        {
            return this.CassandraDataExplorer.GetTimeIndexsColumnValues(columnFamily, columnName, keys, from, to, limit, ascending);
        }

        public virtual List<string> GetTimeIndexsColumnValues(string columnFamily, string columnName, List<string> keys, string from, string to, int limit, bool ascending)
        {
            return this.CassandraDataExplorer.GetTimeIndexsColumnValues(columnFamily, columnName, keys, from, to, limit, ascending);
        }

        public virtual bool DeleteIndex(string columnFamily, string key)
        {
            return this.CassandraDataExplorer.DeleteIndex(columnFamily, key);
        }

        public virtual bool DeleteIndex(string columnFamily, string key, string indexKey)
        {
            return this.CassandraDataExplorer.DeleteIndex(columnFamily, key, indexKey);
        }

        public virtual bool DeleteIndex(string columnFamily, string columnName, string key, string indexKey)
        {
            return this.CassandraDataExplorer.DeleteIndex(columnFamily, columnName, key, indexKey);
        }

        public virtual bool DeleteTimeIndex(string columnFamily, string key, string indexKey)
        {
            return this.CassandraDataExplorer.DeleteTimeIndex(columnFamily, key, indexKey);
        }

        public virtual bool DeleteTimeIndex(string columnFamily, string columnName, string key, string indexKey)
        {
            return this.CassandraDataExplorer.DeleteTimeIndex(columnFamily, columnName, key, indexKey);
        }

        public virtual int Count(string columnFamily, string key)
        {
            return this.CassandraDataExplorer.Count(columnFamily, key);
        }

        public virtual int Count(string columnFamily, string key, int limit)
        {
            return this.CassandraDataExplorer.Count(columnFamily, key, limit);
        }

        public virtual Guid GetTimeBasedGuid(DateTime dt)
        {
            return this.CassandraDataExplorer.GetTimeBasedGuid(dt);
        }

        public virtual DateTime GetDateFromGuid(Guid guid)
        {
            return this.CassandraDataExplorer.GetDateFromGuid(guid);
        }

        [Obsolete("use other ExecuteQuery method with request columns as parameters")]
        public List<T> ExecuteQuery<T>(string query)
        {
            return this.CassandraDataExplorer.ExecuteQuery<T>(query);
        }

        public List<T> ExecuteQueryForSOLR<T>(string columnFamily, List<string> requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit)
        {
            return this.CassandraDataExplorer.ExecuteQueryForSOLR<T>(columnFamily, requestColumn, whereClause, limit);
        }

        public List<T> ExecuteQueryToGetOriginalDataRows(string columnFamily, List<string> requestColumn, Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne = false, bool executeWithCLLocalQuorum = false, bool executeWithCLLocalOne = false)
        {
            return this.CassandraDataExplorer.ExecuteQueryToGetOriginalDataRows<T>(columnFamily, requestColumn, whereClause, limit, executeWithCLOne, executeWithCLLocalQuorum, executeWithCLLocalOne);
        }

        public List<T> ExecuteQuery<T>(string columnFamily, List<string> requestColumn,
        Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne = false,
        bool executeWithCLLocalQuorum = false)
        {
            return this.CassandraDataExplorer.ExecuteQuery<T>(columnFamily, requestColumn, whereClause, limit, executeWithCLOne, executeWithCLLocalQuorum);
        }

        /// <summary>
        /// Execute query but allows for a range query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="columnFamily"></param>
        /// <param name="requestColumn"></param>
        /// <param name="whereClause"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="limit"></param>
        /// <param name="executeWithCLOne"></param>
        /// <param name="executeWithCLLocalQuorum"></param>
        /// <returns></returns>
        public List<T> ExecuteQuery<T>(string columnFamily, List<string> requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, string timeStampColumnName, DateTime start, DateTime end, int limit, bool executeWithCLOne = false,
            bool executeWithCLLocalQuorum = false)
        {
            return this.CassandraDataExplorer.ExecuteQuery<T>(columnFamily, requestColumn, whereClause, timeStampColumnName, start, end, limit, executeWithCLOne, executeWithCLLocalQuorum);
        }

        public List<string> GetColumnValues(string columnFamily, string requestColumn, Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne,
            bool executeWithCLLocalQuorum)
        {
            return this.CassandraDataExplorer.GetColumnValues(columnFamily, requestColumn, whereClause, limit, executeWithCLOne, executeWithCLLocalQuorum);
        }

        public bool ExecuteNonQuery(string columnFamily, List<RowIndex> indexes)
        {
            return this.CassandraDataExplorer.ExecuteNonQuery(columnFamily, indexes);
        }

        public bool ExecuteNonQuery(string columnFamily, List<RowIndex> indexes, uint timeToLive)
        {
            return this.CassandraDataExplorer.ExecuteNonQuery(columnFamily, indexes, timeToLive);
        }

        public virtual void Dispose()
        {
        }

        [Obsolete(DeprecationMessage)]
        public virtual T Get(string key, int retryCount, bool resetDBConnection)
        {
            return this.CassandraDataExplorer.Get<T>(key, retryCount, resetDBConnection);
        }

        [Obsolete(DeprecationMessage)]
        public virtual T Get(string columnFamily, string key, int retryCount, bool resetDBConnection)
        {
            return this.CassandraDataExplorer.Get<T>(columnFamily, key, retryCount, resetDBConnection);
        }

        [Obsolete(DeprecationMessage)]
        public virtual List<T> Get(List<string> keys, int retryCount, bool resetDBConnection)
        {
            return this.CassandraDataExplorer.Get<T>(keys, retryCount, resetDBConnection);
        }

        [Obsolete(DeprecationMessage)]
        public virtual List<T> Get(string columnFamily, List<string> keys, int retryCount, bool resetDBConnection)
        {
            return this.CassandraDataExplorer.Get<T>(columnFamily, keys, retryCount, resetDBConnection);
        }

        [Obsolete(DeprecationMessage)]
        public virtual bool AddIndexes(string columnFamily, List<Index> indexes, uint timeToLive, int retryCount = 0)
        {
            return this.CassandraDataExplorer.AddIndexes(columnFamily, indexes, timeToLive, retryCount);
        }

        public class Data
        {
            public string Key { get; set; }
            public T Value { get; set; }
        }
    }
}
