using automation.core.components.data.v1.Redis;
using automation.core.components.operations.v1;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace automation.core.components.data.v1.Local.Redis
{
    //Local dataexplorer for redis
    public class DataExplorer<T> : IDataExplorer<T>, IDisposable
    {
        public bool Add(string key, string value)
        {
            if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(value))
            {
                LocalContainer.redisData[key] = value;
                return true;
            }
            return false;
        }

        public bool Add(string key, string value, uint timeToLive)
        {
            if (!string.IsNullOrEmpty(key) && !string.IsNullOrEmpty(value))
            {
                LocalContainer.redisData[key] = value;
                return true;
            }
            return false;
        }

         public bool ConnectivityCheck()
        {
            return true;
        }

        public bool Add(string key, T value)
        {
            if (!string.IsNullOrEmpty(key) && value!=null)
            {
                LocalContainer.redisData[key] = JSon.Serialize<T>(value);
                return true;
            }
            return false;
        }

        public bool Add(string key, T value, uint timeToLive)
        {
            if (!string.IsNullOrEmpty(key) && value != null)
            {
                LocalContainer.redisData[key] = JSon.Serialize<T>(value);
                return true;
            }
            return false;
        }

        public bool Add(automation.core.components.data.v1.Redis.DataExplorer<T>.Data data)
        {
            throw new NotImplementedException();
        }

        public bool Add(automation.core.components.data.v1.Redis.DataExplorer<T>.Data data, uint timeToLive)
        {
            throw new NotImplementedException();
        }

        public bool Add(List<automation.core.components.data.v1.Redis.DataExplorer<T>.Data> data)
        {
            throw new NotImplementedException();
        }

        public bool Add(List<automation.core.components.data.v1.Redis.DataExplorer<T>.Data> data, uint timeToLive)
        {
            throw new NotImplementedException();
        }

        public T Get(string key, CommandFlags flag)
        {
            if (LocalContainer.redisData.ContainsKey(key))
            {
                var value = LocalContainer.redisData[key];
                if (!string.IsNullOrEmpty(value))
                {
                    return JSon.Deserialize<T>(value);
                }
            }

            return default(T);
        }

        public string GetValue(string key, CommandFlags flag)
        {
            if (LocalContainer.redisData.ContainsKey(key))
            {
                return LocalContainer.redisData[key];
            }

            return null;
        }

        public List<T> Get(List<string> keys, CommandFlags flag)
        {
            throw new NotImplementedException(); 
        }

        public string GetAsString(string key, CommandFlags flag)
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, string> GetAsString(List<string> keys, CommandFlags flag)
        {
            throw new NotImplementedException();
        }


        public bool Delete(string key)
        {
            throw new NotImplementedException();
        }

        public bool AddIndex(string key, string indexKey, string indexValue)
        {
            if (!LocalContainer.redisIndexData.ContainsKey(key))
            {
                LocalContainer.redisIndexData[key] = new Dictionary<string, string>();
                LocalContainer.redisIndexData[key].Add(indexKey, indexValue);
            }
            else
            {
                (LocalContainer.redisIndexData[key])[indexKey] = indexValue;
            }
            return true;
        }


        public bool AddIndex(string key, string indexKey, string indexValue, uint timeToLive) { throw new NotImplementedException(); }

        public bool AddIndexes(Index index) { throw new NotImplementedException(); }
        public bool AddIndexes(Index index, uint timeToLive) { throw new NotImplementedException(); }
        public bool AddIndexes(List<Index> Indexes) {
            if (Indexes != null && Indexes.Count>0)
            {
                Indexes.ForEach(x => {
                    if (x.KeyValues != null && x.KeyValues.Count > 0)
                    {
                        x.KeyValues.ToList().ForEach(y =>
                        {
                            AddIndex(x.Name, y.Key, y.Value);
                        });
                    }
                });
               
                return true;
            }
            else
                return false;
        }
        public bool AddIndexes(List<Index> Indexes, uint timeToLive) { throw new NotImplementedException(); }

        public Dictionary<string, string> GetIndex(string key, CommandFlags flag) {

            if (LocalContainer.redisIndexData.ContainsKey(key))
            {
                return LocalContainer.redisIndexData[key];
            }

            return new Dictionary<string, string>();
        }
        public Dictionary<string, string> GetIndex(string key, int limit, CommandFlags flag) { throw new NotImplementedException(); }
        public List<string> GetIndexColumnNames(string key, CommandFlags flag) { throw new NotImplementedException(); }
        public List<string> GetIndexColumnNames(string key, int limit, CommandFlags flag) { throw new NotImplementedException(); }
        public List<string> GetIndexColumnValues(string key, CommandFlags flag) { throw new NotImplementedException(); }
        public List<string> GetIndexColumnValues(string key, int limit, CommandFlags flag) { throw new NotImplementedException(); }

        public string GetIndexValue(string key, string indexKey, CommandFlags flag) {

            if (LocalContainer.redisIndexData.ContainsKey(key))
            {
                var keyValuepair = LocalContainer.redisIndexData[key];
                if (keyValuepair != null && keyValuepair.ContainsKey(indexKey))
                {
                    return keyValuepair[indexKey];
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
           
           // foreach (var dict in LocalContainer.redisIndexData[key])
           // { 
           //     if( indexKey.Equals(dict.Key))
           //     {
           //         return dict.Value;
           //     }
           // }

           //return null;
        }

        public bool DeleteIndex(string key) { throw new NotImplementedException(); }

        public bool DeleteIndex(string key, string indexKey) {
            if (LocalContainer.redisIndexData.ContainsKey(key))
            {
                var keyValuepair = LocalContainer.redisIndexData[key];
                if (keyValuepair != null && keyValuepair.ContainsKey(indexKey))
                {
                    keyValuepair.Remove(indexKey);
                    return true;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        public bool DeleteIndex(string columnName, string key, string indexKey) { throw new NotImplementedException(); }

        public int Count(string key, int limit, CommandFlags flag) { throw new NotImplementedException(); }

        public Guid GetTimeBasedGuid(DateTime dt) { throw new NotImplementedException(); }
       
        #region IDisposable Members::

        public virtual void Dispose()
        {

        }

        public T Get(string key)
        {
            return this.Get(key, CommandFlags.None);
        }

        public string GetValue(string key)
        {
            return this.GetValue(key, CommandFlags.None);
        }

        public List<T> Get(List<string> keys)
        {
            return this.Get(keys, CommandFlags.None);
        }

        public string GetAsString(string key)
        {
            return this.GetAsString(key, CommandFlags.None);
        }

        public Dictionary<string, string> GetAsString(List<string> keys)
        {
            return this.GetAsString(keys, CommandFlags.None);
        }

        public Dictionary<string, string> GetIndex(string key)
        {
            return this.GetIndex(key, 1000, CommandFlags.None);
        }

        public Dictionary<string, string> GetIndex(string key, int limit)
        {
            return this.GetIndex(key, 1000, CommandFlags.None);
        }

        public List<string> GetIndexColumnNames(string key)
        {
            return this.GetIndexColumnNames(key, 1000, CommandFlags.None);
        }

        public List<string> GetIndexColumnNames(string key, int limit)
        {
            return this.GetIndexColumnNames(key, 1000, CommandFlags.None);
        }

        public List<string> GetIndexColumnValues(string key)
        {
            return this.GetIndexColumnValues(key, 1000, CommandFlags.None);
        }

        public List<string> GetIndexColumnValues(string key, int limit)
        {
            return this.GetIndexColumnValues(key, 1000, CommandFlags.None);
        }

        public string GetIndexValue(string key, string indexKey)
        {
            return this.GetIndexValue(key, indexKey, CommandFlags.None);
        }

        public int Count(string key)
        {
            return this.Count(key, 1000, CommandFlags.None);
        }

        public int Count(string key, int limit)
        {
            return this.Count(key, 1000, CommandFlags.None);
        }

        #endregion

    }


}
