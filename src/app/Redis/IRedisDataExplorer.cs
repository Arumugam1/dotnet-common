using automation.components.data.v1.Entities;
using System;
using System.Collections.Generic;
using StackExchange.Redis;

namespace automation.components.data.v1.Redis
{
    /// <summary>
    /// Redis Data Explorer Interface
    /// </summary>
    public interface IRedisDataExplorer
    {
        /// <summary>
        /// Add object to db
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">value</param>
        /// <returns>true or false</returns>
        bool Add(string key, string value);

        /// <summary>
        /// Add object to db
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="value">value</param>
        /// <param name="timeToLive">time to live</param>
        /// <returns>true or false</returns>
        bool Add(string key, string value, uint timeToLive);

        /// <summary>
        /// Add object to db
        /// </summary>
        /// <typeparam name="T">Dynamic object</typeparam>
        /// <param name="key">key</param>
        /// <param name="value">value</param>
        /// <returns>true or false</returns>
        bool Add<T>(string key, T value);

        /// <summary>
        /// Add object to db
        /// </summary>
        /// <typeparam name="T">Dynamic object</typeparam>
        /// <param name="key">key</param>
        /// <param name="value">value</param>
        /// <param name="timeToLive">time to live</param>
        /// <returns>true or false</returns>
        bool Add<T>(string key, T value, uint timeToLive);

        /// <summary>
        /// Add object to db
        /// </summary>
        /// <typeparam name="T">Dynamic object</typeparam>
        /// <param name="data">list of entity</param>
        /// <returns>true or false</returns>
        bool Add<T>(List<RedisEntity<T>> data);

        /// <summary>
        /// Add object to db
        /// </summary>
        /// <typeparam name="T">Dynamic object</typeparam>
        /// <param name="data">list of entity</param>
        /// <param name="timeToLive">TTL</param>
        /// <returns>true or false</returns>
        bool Add<T>(List<RedisEntity<T>> data, uint timeToLive);

        /// <summary>
        /// get objects by key
        /// </summary>
        /// <typeparam name="T">Dynamic object</typeparam>
        /// <param name="key">key</param>
        /// <returns>object</returns>
        T Get<T>(string key);

        /// <summary>
        /// get value
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>string</returns>
        string GetValue(string key);

        /// <summary>
        /// get value
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="flag">flag</param>
        /// <returns>string</returns>
        string GetValue(string key, CommandFlags flag);

        /// <summary>
        /// list of objects
        /// </summary>
        /// <typeparam name="T">Dynamic object</typeparam>
        /// <param name="keys">list of keys</param>
        /// <returns>list of object</returns>
        List<T> Get<T>(List<string> keys);

        /// <summary>
        /// list of objects
        /// </summary>
        /// <typeparam name="T">Dynamic object</typeparam>
        /// <param name="keys">list of keys</param>
        /// <param name="flag">flag</param>
        /// <returns>list of object</returns>
        List<T> Get<T>(List<string> keys, CommandFlags flag);

        /// <summary>
        /// Get the redis index by ID
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>string</returns>
        string GetAsString(string key);

        /// <summary>
        /// Get the redis indexes by using keys
        /// </summary>
        /// <param name="keys">list of key</param>
        /// <returns>list of object</returns>
        Dictionary<string, string> GetAsString(List<string> keys);

        /// <summary>
        /// Get the redis indexes by using keys
        /// </summary>
        /// <param name="keys">list of key</param>
        /// <param name="flag">flag</param>
        /// <returns>list of object</returns>
        Dictionary<string, string> GetAsString(List<string> keys, CommandFlags flag);

        /// <summary>
        /// Delete redis index by key
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>true or false</returns>
        bool Delete(string key);

        /// <summary>
        /// Add index to redis
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="indexKey">indexkey</param>
        /// <param name="indexValue">value</param>
        /// <returns>true or false</returns>
        bool AddIndex(string key, string indexKey, string indexValue);

        /// <summary>
        /// Add index to redis
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="indexKey">indexkey</param>
        /// <param name="indexValue">true or false</param>
        /// <param name="timeToLive">Time to Live</param>
        /// <returns>true false</returns>
        bool AddIndex(string key, string indexKey, string indexValue, uint timeToLive);

        /// <summary>
        /// Add index to redis
        /// </summary>
        /// <param name="index">entity</param>
        /// <returns>true or false</returns>
        bool AddIndexes(Index index);

        /// <summary>
        /// Add index to redis
        /// </summary>
        /// <param name="index">entity</param>
        /// <param name="timeToLive">TTL</param>
        /// <returns>true or false</returns>
        bool AddIndexes(Index index, uint timeToLive);

        /// <summary>
        /// Add index to redis
        /// </summary>
        /// <param name="indexes">list of entity</param>
        /// <returns>true or false</returns>
        bool AddIndexes(List<Index> indexes);

        /// <summary>
        /// Add index to redis
        /// </summary>
        /// <param name="indexes">list of entity</param>
        /// <param name="timeToLive">TTL</param>
        /// <returns>true or false</returns>
        bool AddIndexes(List<Index> indexes, uint timeToLive);

        /// <summary>
        /// Get object by ID
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>object</returns>
        Dictionary<string, string> GetIndex(string key);

        /// <summary>
        /// Get object by ID with limit
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="limit">limit</param>
        /// <returns>object</returns>
        Dictionary<string, string> GetIndex(string key, int limit);

        /// <summary>
        /// Get index column names
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>list of index column names</returns>
        List<string> GetIndexColumnNames(string key);

        /// <summary>
        /// Get index column names with limit
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="limit">limit</param>
        /// <returns>list of index column names</returns>
        List<string> GetIndexColumnNames(string key, int limit);

        /// <summary>
        /// Get index column values with limit
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>list of index column values</returns>
        List<string> GetIndexColumnValues(string key);

        /// <summary>
        /// Get index column values with limit
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="limit">limit</param>
        /// <returns>list of index column values</returns>
        List<string> GetIndexColumnValues(string key, int limit);

        /// <summary>
        /// Get index value
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="indexKey">index key</param>
        /// <returns>list of index</returns>
        string GetIndexValue(string key, string indexKey);

        /// <summary>
        /// Get redis index
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="limit">limit</param>
        /// <param name="flag">flag</param>
        /// <returns>list of index</returns>
        Dictionary<string, string> GetIndex(string key, int limit, CommandFlags flag);

        /// <summary>
        /// Get Redis Index Column Names
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="limit">limit</param>
        /// <param name="flag">flag</param>
        /// <returns>list of index column names</returns>
        List<string> GetIndexColumnNames(string key, int limit, CommandFlags flag);

        /// <summary>
        /// Get Redis Index Column Values
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="limit">limit</param>
        /// <param name="flag">flag</param>
        /// <returns>list of index column values</returns>
        List<string> GetIndexColumnValues(string key, int limit, CommandFlags flag);

        /// <summary>
        /// Get Redis Index Value
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="indexKey">index key</param>
        /// <param name="flag">flag</param>
        /// <returns>index value</returns>
        string GetIndexValue(string key, string indexKey, CommandFlags flag);

        /// <summary>
        /// Delete redis index by key
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>true or false</returns>
        bool DeleteIndex(string key);

        /// <summary>
        /// Delete redis index by key, index key
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="indexKey">index key</param>
        /// <returns>true or false</returns>
        bool DeleteIndex(string key, string indexKey);

        /// <summary>
        /// Delete redis index by columnName, key, index key
        /// </summary>
        /// <param name="columnName">Column Name</param>
        /// <param name="key">key</param>
        /// <param name="indexKey">index key</param>
        /// <returns>true or false</returns>
        bool DeleteIndex(string columnName, string key, string indexKey);

        /// <summary>
        /// Get the count for the key
        /// </summary>
        /// <param name="key">key</param>
        /// <returns>count</returns>
        int Count(string key);

        /// <summary>
        /// Get the count for the key with limit
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="limit">limit</param>
        /// <returns>count</returns>
        int Count(string key, int limit);

        /// <summary>
        /// Get the count for the key with limit
        /// </summary>
        /// <param name="key">key</param>
        /// <param name="limit">limt</param>
        /// <param name="flag">flag</param>
        /// <returns>count</returns>
        int Count(string key, int limit, CommandFlags flag);

        /// <summary>
        /// Get Time Based Guid
        /// </summary>
        /// <param name="dt">date time</param>
        /// <returns>guid for datetime</returns>
        Guid GetTimeBasedGuid(DateTime dt);

        /// <summary>
        /// connectivity check for redis
        /// </summary>
        /// <returns>true or false</returns>
        bool ConnectivityCheck();
    }
}