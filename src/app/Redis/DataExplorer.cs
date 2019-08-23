using System;
using System.Collections.Generic;
using StackExchange.Redis;
using automation.components.data.v1.Config;
using automation.components.data.v1.Entities;
using automation.components.operations.v1;

namespace automation.components.data.v1.Redis
{
    /// <summary>
    /// Redis data explorer
    /// </summary>
    /// <typeparam name="T">dynamic object</typeparam>
    public abstract class DataExplorer<T> : IDataExplorer<T>, IDisposable
    {
        public IRedisDataExplorer RedisDataExplorer { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataExplorer{T}"/> class.
        /// </summary>
        protected DataExplorer()
        {
            if (Parser.ToBoolean(Manager.GetApplicationConfigValue("Dynomite.Enabled", "AllApplications.Redis"), false))
            {
                this.RedisDataExplorer = new StackExchangeRedisDefaultProvider();
            }
            else
            {
                this.RedisDataExplorer = new StackExchangeRedisSentinelProvider();
            }
        }

        /// <summary>
        /// Add to db
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">value</param>
        /// <returns>trur or false</returns>
        public virtual bool Add(string key, string value)
        {
            return this.Add(key, value, 0);
        }

        public virtual bool Add(string key, string value, uint timeToLive)
        {
            return this.RedisDataExplorer.Add(key, value, timeToLive);
        }

        public virtual bool ConnectivityCheck()
        {
            return this.RedisDataExplorer.ConnectivityCheck();
        }

        public virtual bool Add(string key, T value)
        {
            return this.RedisDataExplorer.Add<T>(key, value, 0);
        }

        public virtual bool Add(string key, T value, uint timeToLive)
        {
            return this.RedisDataExplorer.Add<T>(key, value, timeToLive);
        }

        public virtual bool Add(Data data)
        {
            return this.RedisDataExplorer.Add(data.Key, data.Value, 0);
        }

        public virtual bool Add(Data data, uint timeToLive)
        {
            return this.RedisDataExplorer.Add(data.Key, data.Value, timeToLive);
        }

        public virtual bool Add(List<Data> data)
        {
            List<RedisEntity<T>> redisEntities = new List<RedisEntity<T>>();
            foreach (Data entity in data)
            {
                redisEntities.Add(new RedisEntity<T> { Key = entity.Key, Value = entity.Value });
            }

            return this.RedisDataExplorer.Add(redisEntities);
        }

        public virtual bool Add(List<Data> data, uint timeToLive)
        {
            List<RedisEntity<T>> redisEntities = new List<RedisEntity<T>>();
            foreach (Data entity in data)
            {
                redisEntities.Add(new RedisEntity<T> { Key = entity.Key, Value = entity.Value });
            }

            return this.RedisDataExplorer.Add(redisEntities, timeToLive);
        }

        public virtual T Get(string key)
        {
            return this.RedisDataExplorer.Get<T>(key);
        }

        public virtual string GetValue(string key)
        {
            return this.RedisDataExplorer.GetValue(key);
        }

        public virtual string GetValue(string key, CommandFlags flag)
        {
            return this.RedisDataExplorer.GetValue(key, flag);
        }

        public virtual List<T> Get(List<string> keys)
        {
            return this.RedisDataExplorer.Get<T>(keys);
        }

        public virtual List<T> Get(List<string> keys, CommandFlags flag)
        {
            return this.RedisDataExplorer.Get<T>(keys, flag);
        }

        public virtual string GetAsString(string key)
        {
            return this.RedisDataExplorer.GetAsString(key);
        }

        public virtual Dictionary<string, string> GetAsString(List<string> keys)
        {
            return this.RedisDataExplorer.GetAsString(keys);
        }

        public virtual Dictionary<string, string> GetAsString(List<string> keys, CommandFlags flag)
        {
            return this.RedisDataExplorer.GetAsString(keys, flag);
        }

        public virtual bool Delete(string key)
        {
            return this.RedisDataExplorer.Delete(key);
        }

        public virtual bool AddIndex(string key, string indexKey, string indexValue)
        {
            return this.RedisDataExplorer.AddIndex(key, indexKey, indexValue);
        }

        public virtual bool AddIndex(string key, string indexKey, string indexValue, uint timeToLive)
        {
            return this.RedisDataExplorer.AddIndex(key, indexKey, indexValue, timeToLive);
        }

        public virtual bool AddIndexes(Index index)
        {
            return this.RedisDataExplorer.AddIndexes(index);
        }

        public virtual bool AddIndexes(Index index, uint timeToLive)
        {
            return this.RedisDataExplorer.AddIndexes(index, timeToLive);
        }

        public virtual bool AddIndexes(List<Index> indexes)
        {
            return this.RedisDataExplorer.AddIndexes(indexes);
        }

        public virtual bool AddIndexes(List<Index> indexes, uint timeToLive)
        {
            return this.RedisDataExplorer.AddIndexes(indexes, timeToLive);
        }

        public virtual Dictionary<string, string> GetIndex(string key)
        {
            return this.RedisDataExplorer.GetIndex(key);
        }

        public virtual Dictionary<string, string> GetIndex(string key, int limit)
        {
            return this.RedisDataExplorer.GetIndex(key, limit);
        }

        public virtual Dictionary<string, string> GetIndex(string key, int limit, CommandFlags flag)
        {
            return this.RedisDataExplorer.GetIndex(key, limit, flag);
        }

        public virtual List<string> GetIndexColumnNames(string key)
        {
            return this.RedisDataExplorer.GetIndexColumnNames(key);
        }

        public virtual List<string> GetIndexColumnNames(string key, int limit)
        {
            return this.RedisDataExplorer.GetIndexColumnNames(key, limit);
        }

        public virtual List<string> GetIndexColumnNames(string key, int limit, CommandFlags flag)
        {
            return this.RedisDataExplorer.GetIndexColumnNames(key, limit, flag);
        }

        public virtual List<string> GetIndexColumnValues(string key)
        {
            return this.RedisDataExplorer.GetIndexColumnValues(key);
        }

        public virtual List<string> GetIndexColumnValues(string key, int limit)
        {
            return this.RedisDataExplorer.GetIndexColumnValues(key, limit);
        }

        public virtual List<string> GetIndexColumnValues(string key, int limit, CommandFlags flag)
        {
            return this.RedisDataExplorer.GetIndexColumnValues(key, limit, flag);
        }

        public virtual string GetIndexValue(string key, string indexKey)
        {
            return this.RedisDataExplorer.GetIndexValue(key, indexKey);
        }

        public virtual string GetIndexValue(string key, string indexKey, CommandFlags flag)
        {
            return this.RedisDataExplorer.GetIndexValue(key, indexKey, flag);
        }

        public virtual bool DeleteIndex(string key)
        {
            return this.RedisDataExplorer.DeleteIndex(key);
        }

        public virtual bool DeleteIndex(string key, string indexKey)
        {
            return this.RedisDataExplorer.DeleteIndex(key, indexKey);
        }

        public virtual bool DeleteIndex(string columnName, string key, string indexKey)
        {
            return this.RedisDataExplorer.DeleteIndex(columnName, key, indexKey);
        }

        public virtual int Count(string key)
        {
            return this.RedisDataExplorer.Count(key);
        }

        public virtual int Count(string key, int limit)
        {
            return this.RedisDataExplorer.Count(key, limit);
        }

        public virtual int Count(string key, int limit, CommandFlags flag)
        {
            return this.RedisDataExplorer.Count(key, limit, flag);
        }

        public virtual Guid GetTimeBasedGuid(DateTime dt)
        {
            return this.RedisDataExplorer.GetTimeBasedGuid(dt);
        }

        public virtual void Dispose()
        {
        }

        public class Data
        {
            public string Key { get; set; }

            public T Value { get; set; }
        }
    }
}
