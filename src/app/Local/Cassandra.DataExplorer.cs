using System;
using System.Collections.Generic;
using System.Linq;
using automation.components.operations.v1;
using cass = automation.components.data.v1.Cassandra;
using automation.components.data.v1.Entities;
using db = Dse;
using automation.components.operations.v1.JSonExtensions.Linq;
using System.Linq.Dynamic;
using System.Text.RegularExpressions;
using System.Dynamic;

namespace automation.components.data.v1.Local.Cassandra
{
    public class DataExplorer<T> : cass.IDataExplorer<T>, IDisposable
    {
        #region Private Members::

        internal string _keyspace { get; set; }
        internal string _columnFamily { get; set; }
        internal string _keyPrefix { get; set; }
        #endregion

        public DataExplorer()
        {
        }

        public DataExplorer(string keyspace, string columnFamily, string keyPrefix)
        {
            this._keyspace = keyspace;
            this._columnFamily = columnFamily;
            this._keyPrefix = keyPrefix;
        }

        private bool AddData(string cf, string key, string value)
        {
            if (string.IsNullOrEmpty(key) || string.IsNullOrEmpty(value))
                return false;

            if (!LocalContainer.data.ContainsKey(cf))
                LocalContainer.data.Add(cf, new Dictionary<string, Dictionary<string, string>>());

            if (!LocalContainer.data[cf].ContainsKey(key))
                LocalContainer.data[cf].Add(key, new Dictionary<string, string>());

            if (!LocalContainer.data[cf][key].ContainsKey("json"))
                LocalContainer.data[cf][key].Add("json", value);
            else
                LocalContainer.data[cf][key]["json"] = value;

            return true;
        }

        private bool AddData(string cf, string key, string columnname, string columnvalue)
        {
            if (string.IsNullOrEmpty(key))
                return false;

            if (!LocalContainer.data.ContainsKey(cf))
                LocalContainer.data.Add(cf, new Dictionary<string, Dictionary<string, string>>());

            if (!LocalContainer.data[cf].ContainsKey(key))
                LocalContainer.data[cf].Add(key, new Dictionary<string, string>());

            if (!LocalContainer.data[cf][key].ContainsKey(columnname))
                LocalContainer.data[cf][key].Add(columnname, columnvalue);
            else
                LocalContainer.data[cf][key][columnname] = columnvalue;

            return true;
        }

        public List<T> ExecuteQuery<T>(string query)
        {
            query = query.Replace(";", "");
            int limit = query.Contains(" limit ") ? Convert.ToInt32(query.Substring(query.IndexOf(" limit ") + 7)) : 0;
            if (query.Contains("limit"))
                query = query.Substring(0, query.IndexOf("limit"));

            if (query.Contains("order by"))
                query = query.Substring(0, query.IndexOf("order by"));

            string tableName = Regex.Match(query, string.Format("{0}(.*){1}", " from ", " where ")).Groups[1].Value;

            string whereClause = query.Contains(" where ") ? query.Substring(query.IndexOf(" where ") + 7) : string.Empty;
            string pattern = @"(?<=^|\A|(\bAND\b))(?:[^']|'(?:[^']|'{2})+')*?(?=(\bAND\b)|$|\Z)";
            var matches = Regex.Matches(whereClause, pattern, RegexOptions.IgnoreCase);
            Dictionary<string, object> whereColumnValues = new Dictionary<string, object>();
            foreach (Match match in matches)
            {
                string columnName = match.Groups[0].Value.Split('=')[0].Trim();
                string columnValue = match.Groups[0].Value.Split('=')[1].Trim().Trim('\'');
                int columnValueNumber;
                bool columnValueBoolean;
                if (int.TryParse(columnValue, out columnValueNumber))
                    whereColumnValues.Add(columnName, columnValueNumber);
                else if (bool.TryParse(columnValue, out columnValueBoolean))
                    whereColumnValues.Add(columnName, columnValueBoolean);
                else
                    whereColumnValues.Add(columnName, columnValue);
            }

            if (!LocalContainer.data.ContainsKey(tableName))
                return default(List<T>);

            var table = JToken.Parse(LocalContainer.data[tableName]["key"]["json"]);
            var tableDef = JToken.Parse(table["Definition"].ToString());

            if (tableDef == null || !tableDef.Any()) return default(List<T>);
            string orderBy = tableDef["Order By"] != null ? tableDef["Order By"].ToString() : string.Empty;            

            List<T> rows = JSon.Deserialize<List<T>>(table["Rows"].ToString());
            string predicate = string.Join(" and ", whereColumnValues.Keys.Select((x, index) => string.Format("{0} == @{1}", x.Trim(), index)));

            var response = !string.IsNullOrEmpty(orderBy) ?
                rows.Where(predicate, whereColumnValues.Values.ToArray()).OrderBy(orderBy) :
                rows.Where(predicate, whereColumnValues.Values.ToArray());

            return response != null && response.Any() && limit > 0 ? response.Take(limit).ToList() : response.ToList();
        }

        public bool ExecuteNonQuery(string columnFamily, List<cass.RowIndex> indexes)
        {
            return this.ExecuteNonQuery(columnFamily, indexes, 0);
        }

        public bool ExecuteNonQuery(string columnFamily, List<cass.RowIndex> indexes, uint timeToLive)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) ||
                indexes == null || !indexes.Any())
                return false;

            var table = JToken.Parse(LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"]);
            List<T> tableRows = JSon.Deserialize<List<T>>(table["Rows"].ToString());
            Dictionary<string, object> definition = JSon.Deserialize<Dictionary<string, object>>(table["Definition"].ToString());            

            List<string> primaryKeys = definition != null && definition.Any() && definition["Primary Key"] != null ? 
                definition["Primary Key"].ToString().Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).ToList() : new List<string>();
                
            foreach(var rowIndex in indexes)
            {
                if (rowIndex.EnableAutoTimeUUID)
                {
                    if (rowIndex.KeyValues.ContainsKey("modification_timestamp"))
                        rowIndex.KeyValues["modification_timestamp"] = this.GetTimeBasedGuid(DateTime.UtcNow);
                    else
                        rowIndex.KeyValues.Add("modification_timestamp", this.GetTimeBasedGuid(DateTime.UtcNow));
                }

                tableRows.Add(JSon.Deserialize<T>(JSon.Serialize(rowIndex.KeyValues)));
            }


            Dictionary<string, object> result = new Dictionary<string, object>
            {
                {"Definition", definition},
                {"Rows", tableRows}
            };
            LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"] = JSon.Serialize(result);

            return true;
        }

        public bool Add(string key, T value)
        {
            return this.AddData(this._keyspace + "." + this._columnFamily, key, JSon.Serialize(value));
        }

        public bool Add(string key, T value, uint timeToLive)
        {
            return this.AddData(this._keyspace + "." + this._columnFamily, key, JSon.Serialize(value));
        }
        
        public bool ConnectivityCheck()
        {
            return true;
        }
        
        public bool Add(cass.DataExplorer<T>.Data data)
        {
            return this.Add(data.Key, data.Value);
        }

        public bool Add(cass.DataExplorer<T>.Data data, uint timeToLive)
        {
            return this.Add(data.Key, data.Value);
        }

        public bool Add(List<cass.DataExplorer<T>.Data> data)
        {
            foreach (var curData in data)
                this.Add(curData.Key, curData.Value);

            return true;
        }

        public bool Add(List<cass.DataExplorer<T>.Data> data, uint timeToLive)
        {
            foreach (var curData in data)
                this.Add(curData.Key, curData.Value);

            return true;
        }

        public T Get(string key)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily) || !LocalContainer.data[this._keyspace + "." + this._columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + this._columnFamily][key].ContainsKey("json"))
                return default(T);

            return JSon.Deserialize<T>(LocalContainer.data[this._keyspace + "." + this._columnFamily][key]["json"]);
        }

        public List<T> Get(List<string> keys)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily))
                return new List<T>();

            var result = new List<T>();
            foreach (string key in keys)
            {
                if ((LocalContainer.data[this._keyspace + "." + this._columnFamily].ContainsKey(key) && LocalContainer.data[this._keyspace + "." + this._columnFamily][key].ContainsKey("json")))
                {
                    result.Add(JSon.Deserialize<T>(LocalContainer.data[this._keyspace + "." + this._columnFamily][key]["json"]));
                }
            }
            //return JSon.Deserialize<List<T>>(JSon.Serialize(LocalContainer.data[_keyspace + "." + _columnFamily].Values));
            return result;
        }


        public T Get(string columnFamily, string key)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey("json"))
                return default(T);

            return JSon.Deserialize<T>(LocalContainer.data[this._keyspace + "." + columnFamily][key]["json"]);
        }

        public List<T> Get(string columnFamily, List<string> keys)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return new List<T>();

            var result = new List<T>();
            foreach (string key in keys)
            {
                if ((LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) && LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey("json")))
                {
                    result.Add(JSon.Deserialize<T>(LocalContainer.data[this._keyspace + "." + columnFamily][key]["json"]));
                }
            }
            return result;
        }

        public T Get(string key, bool executeInSingleNode)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily) || !LocalContainer.data[this._keyspace + "." + this._columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + this._columnFamily][key].ContainsKey("json"))
                return default(T);

            return JSon.Deserialize<T>(LocalContainer.data[this._keyspace + "." + this._columnFamily][key]["json"]);
        }

        public List<T> Get(List<string> keys, bool executeInSingleNode)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily))
                return new List<T>();

            var result = new List<T>();
            foreach (string key in keys)
            {
                if ((LocalContainer.data[this._keyspace + "." + this._columnFamily].ContainsKey(key) && LocalContainer.data[this._keyspace + "." + this._columnFamily][key].ContainsKey("json")))
                {
                    result.Add(JSon.Deserialize<T>(LocalContainer.data[this._keyspace + "." + this._columnFamily][key]["json"]));
                }
            }
            return result;
        }

        public List<string> GetKeys()
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily))
                return new List<string>();

            return LocalContainer.data[this._keyspace + "." + this._columnFamily].Keys.ToList();
        }

        public List<T> Get(int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily))
                return new List<T>();

            var result = new List<T>();
            if (limit > 0)
            {
                IEnumerable<Dictionary<string, string>> list = LocalContainer.data[this._keyspace + "." + this._columnFamily].Values.Take(limit);

                foreach (Dictionary<string, string> dict in list)
                {
                    result.AddRange(dict.Select(keyValuePair => JSon.Deserialize<T>(keyValuePair.Value)));
                }
            }
            else
            {
                IEnumerable<Dictionary<string, string>> list = LocalContainer.data[this._keyspace + "." + this._columnFamily].Values.Take(1000);

                foreach (Dictionary<string, string> dict in list)
                {
                    result.AddRange(dict.Select(keyValuePair => JSon.Deserialize<T>(keyValuePair.Value)));
                }
            }

            return result;
        }

        public string GetAsString(string key)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily) || !LocalContainer.data[this._keyspace + "." + this._columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + this._columnFamily][key].ContainsKey("json"))
                return default(string);

            return JSon.Deserialize<string>(LocalContainer.data[this._keyspace + "." + this._columnFamily][key]["json"]);
        }

        public Dictionary<string, string> GetAsString(List<string> keys)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily))
                return default(Dictionary<string, string>);

            return LocalContainer.data[this._keyspace + "." + this._columnFamily]
                .Where(x => keys.Contains(x.Key) && x.Value != null && x.Value.Keys.Contains("json"))
                .ToDictionary(x => x.Key, x => x.Value["json"]);
        }

        public Dictionary<string, string> GetAsString(string columnFamily, List<string> keys)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(Dictionary<string, string>);

            return LocalContainer.data[this._keyspace + "." + columnFamily]
                .Where(x => keys.Contains(x.Key) && x.Value != null && x.Value.Count > 0)
                .ToDictionary(x => x.Key, x => x.Value.FirstOrDefault().Value);
        }

        public string GetAsString(string key, bool executeInSingleNode)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily) || !LocalContainer.data[this._keyspace + "." + this._columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + this._columnFamily][key].ContainsKey("json"))
                return default(string);

            return LocalContainer.data[this._keyspace + "." + this._columnFamily][key]["json"];
        }

        public Dictionary<string, string> GetAsString(string columnFamily, int limit)
        {
            return this.GetAsString(columnFamily, limit, false);
        }

        public Dictionary<string, string> GetAsString(string columnFamily, int limit, bool executeInSingleNode)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(Dictionary<string, string>);

            return LocalContainer.data[this._keyspace + "." + this._columnFamily].ToDictionary(x => x.Key, x => x.Value.FirstOrDefault().Value);
        }

        public Dictionary<string, string> GetAsString(List<string> keys, bool executeInSingleNode)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily))
                return default(Dictionary<string, string>);

            return LocalContainer.data[this._keyspace + "." + this._columnFamily]
                .Where(x => keys.Contains(x.Key) && x.Value != null && x.Value.Keys.Contains("json"))
                .ToDictionary(x => x.Key, x => x.Value["json"]);
        }

        public Dictionary<string, string> GetAsString(string columnFamily, List<string> keys, bool executeInSingleNode)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(Dictionary<string, string>);

            return LocalContainer.data[this._keyspace + "." + columnFamily]
                .Where(x => keys.Contains(x.Key) && x.Value != null && x.Value.Count > 0)
                .ToDictionary(x => x.Key, x => x.Value.FirstOrDefault().Value);
        }

        public List<string> Filter(string columnName, DateTime to)
        {
            throw new NotImplementedException();
        }

        public List<string> Filter(string columnName, DateTime from, DateTime to)
        {
            throw new NotImplementedException();
        }

        public List<string> Filter(string columnName, DateTime to, int limit)
        {
            throw new NotImplementedException();
        }

        public List<string> Filter(string columnName, DateTime from, DateTime to, int limit)
        {
            throw new NotImplementedException();
        }

        public bool Delete(string key)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + this._columnFamily) || !LocalContainer.data[this._keyspace + "." + this._columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + this._columnFamily][key].ContainsKey("json"))
                return true;

            return LocalContainer.data[this._keyspace + "." + this._columnFamily].Remove(key);
        }

        public bool AddIndex(string columnFamily, Dictionary<string, string> keyValues)
        {
            throw new NotImplementedException();
        }

        public bool AddIndex(string columnFamily, Dictionary<string, string> keyValues, uint timeToLive)
        {
            throw new NotImplementedException();
        }

        public bool AddIndex(string columnFamily, string key, string indexKey, string indexValue)
        {
            return this.AddData(this._keyspace + "." + columnFamily, key, indexKey, indexValue);
        }

        public bool AddIndex(string columnFamily, string key, string indexKey, string indexValue, uint timeToLive)
        {
            return this.AddIndex(columnFamily, key, indexKey, indexValue);
        }

        public bool AddIndexWithTimeUUID(string columnFamily, string key, string indexKey, string indexValue)
        {
            return this.AddData(this._keyspace + "." + columnFamily, key, indexKey, indexValue);
        }

        public bool AddIndexWithTimeUUID(string columnFamily, string key, string indexKey, string indexValue, uint timeToLive)
        {
            return this.AddIndex(columnFamily, key, indexKey, indexValue);
        }

        public bool AddIndexes(string columnFamily, cass.Index index)
        {
            foreach (var curIndex in index.KeyValues)
                this.AddData(this._keyspace + "." + columnFamily, index.Name, curIndex.Key, curIndex.Value);

            return true;
        }

        public bool AddIndexes(string columnFamily, cass.Index index, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, index);
        }

        public bool AddIndexes(string columnFamily, List<cass.Index> indexes)
        {
            foreach (var index in indexes)
                foreach (var curIndex in index.KeyValues)
                    this.AddData(this._keyspace + "." + columnFamily, index.Name, curIndex.Key, curIndex.Value);

            return true;
        }

        public bool AddIndexes(string columnFamily, List<cass.Index> indexes, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, indexes);
        }

        public bool AddIndexesWithTimeUUID(string columnFamily, List<cass.Index> indexes)
        {
            foreach (var index in indexes)
                foreach (var curIndex in index.KeyValues)
                    this.AddData(this._keyspace + "." + columnFamily, index.Name, curIndex.Key, curIndex.Value);

            return true;
        }

        public bool AddIndexesWithTimeUUID(string columnFamily, List<cass.Index> indexes, uint timeToLive)
        {
            return this.AddIndexesWithTimeUUID(columnFamily, indexes);
        }

        public bool AddIndexes(string columnFamily, cass.TimeIndex index)
        {
            foreach (var curIndex in index.KeyValues)
                this.AddData(this._keyspace + "." + columnFamily, index.Name, curIndex.Key.ToString(), curIndex.Value);

            return true;
        }

        public bool AddIndexes(string columnFamily, cass.TimeIndex index, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, index);
        }

        public bool AddIndexes(string columnFamily, List<cass.TimeIndex> indexes)
        {
            foreach (var index in indexes)
                foreach (var curIndex in index.KeyValues)
                    this.AddData(this._keyspace + "." + columnFamily, index.Name, curIndex.Key.ToString(), curIndex.Value);

            return true;
        }

        public bool AddIndexes(string columnFamily, List<cass.TimeIndex> indexes, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, indexes);
        }

        public bool AddIndexes(string columnFamily, cass.TimeUUIDIndex index)
        {
            foreach (var curIndex in index.KeyValues)
                this.AddData(this._keyspace + "." + columnFamily, index.Name, curIndex.Key.ToString(), curIndex.Value);

            return true;
        }

        public bool AddIndexes(string columnFamily, cass.TimeUUIDIndex index, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, index);
        }

        public bool AddIndexes(string columnFamily, List<cass.TimeUUIDIndex> indexes)
        {
            foreach (var index in indexes)
                foreach (var curIndex in index.KeyValues)
                    this.AddData(this._keyspace + "." + columnFamily, index.Name, curIndex.Key.ToString(), curIndex.Value);

            return true;
        }

        public bool AddIndexes(string columnFamily, List<cass.TimeUUIDIndex> indexes, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, indexes);
        }

        public Dictionary<string, string> GetIndex(string columnFamily, string key)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return default(Dictionary<string, string>);

            return LocalContainer.data[this._keyspace + "." + columnFamily][key];
        }

        public Dictionary<string, string> GetUniqueIndex(string columnFamily, string key)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return default(Dictionary<string, string>);

            return LocalContainer.data[this._keyspace + "." + columnFamily][key];
        }

        public Dictionary<string, string> GetIndex(string columnFamily, string key, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return default(Dictionary<string, string>);

            if (limit > 0)
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Take(limit).ToDictionary(x => x.Key, x => x.Value);
            else
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].ToDictionary(x => x.Key, x => x.Value);
        }

        public Dictionary<string, string> GetUniqueIndex(string columnFamily, string key, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return default(Dictionary<string, string>);

            if (limit > 0)
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Take(limit).ToDictionary(x => x.Key, x => x.Value);
            else
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].ToDictionary(x => x.Key, x => x.Value);
        }

        public List<string> GetIndexColumnNames(string columnFamily, string key)
        {
            var indexes = this.GetIndex(columnFamily, key);
            if (indexes != null && indexes.Count > 0)
                return this.GetIndex(columnFamily, key).Keys.ToList();
            return new List<string>();
        }

        public List<string> GetColumnValues(string columnFamily, string requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne,
            bool executeWithCLLocalQuorum)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(requestColumn))
                return default(List<string>);

            var values = LocalContainer.data[this._keyspace + "." + columnFamily][requestColumn];
            if (values != null && values.Count > 0)
                return this.GetIndex(columnFamily, requestColumn).Keys.ToList();
            return new List<string>();

        }

        public List<string> GetIndexColumnNames(string columnFamily, string key, int limit)
        {
            var indexes = this.GetIndex(columnFamily, key);
            if (indexes != null && indexes.Count > 0)
                return this.GetIndex(columnFamily, key, limit).Keys.ToList();
            return new List<string>();
        }

        public List<string> GetIndexColumnValues(string columnFamily, string key)
        {
            var indexes = this.GetIndex(columnFamily, key);
            if (indexes != null && indexes.Count > 0)
                return this.GetIndex(columnFamily, key).Values.ToList();
            return new List<string>();
        }

        public List<string> GetIndexColumnValues(string columnFamily, string key, int limit)
        {
            var indexes = this.GetIndex(columnFamily, key);
            if (indexes != null && indexes.Count > 0)
                return this.GetIndex(columnFamily, key, limit).Values.ToList();
            return new List<string>();
        }

        public Dictionary<string, string> GetIndex(string columnFamily, string key, string indexKey)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return default(Dictionary<string, string>);

            return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).ToDictionary(x => x.Key, x => x.Value);
        }

        public Dictionary<string, string> GetIndex(string columnFamily, string key, string indexKey, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return default(Dictionary<string, string>);

            if (limit > 0)
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).Take(limit).ToDictionary(x => x.Key, x => x.Value);
            else
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).ToDictionary(x => x.Key, x => x.Value);
        }

        public Dictionary<string, string> GetUniqueIndex(string columnFamily, string key, string indexKey, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return default(Dictionary<string, string>);

            if (limit > 0)
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).Take(limit).ToDictionary(x => x.Key, x => x.Value);
            else
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).ToDictionary(x => x.Key, x => x.Value);
        }

        public List<string> GetIndexColumnNames(string columnFamily, string key, string indexKey)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return default(List<string>);

            return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).Select(x => x.Key).ToList();
        }

        public List<string> GetIndexColumnNames(string columnFamily, string key, string indexKey, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return default(List<string>);

            if (limit > 0)
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).Take(limit).Select(x => x.Key).ToList();
            else
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).Select(x => x.Key).ToList();
        }

        public List<string> GetIndexColumnValues(string columnFamily, string key, string indexKey)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return default(List<string>);

            return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).Select(x => x.Value).ToList();
        }

        public List<string> GetIndexColumnValues(string columnFamily, string key, string indexKey, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) ||
                !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return default(List<string>);

            if (limit > 0)
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).Take(limit).Select(x => x.Value).ToList();
            else
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Where(x => x.Key == indexKey).Select(x => x.Value).ToList();
        }

        public Dictionary<string, string> GetIndexesColumns(string columnFamily)
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, string> GetIndexesColumns(string columnFamily, int limit)
        {
            var result = new Dictionary<string, string>();

            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(Dictionary<string, string>);

            Dictionary<string, Dictionary<string, string>> listOfValues = LocalContainer.data[this._keyspace + "." + columnFamily].ToDictionary(x => x.Key, x => x.Value);

            if (listOfValues != null && listOfValues.Count > 0)
            {
                foreach (var item in listOfValues)
                {
                    result.Add(item.Key, item.Value.First().Key.ToString());
                }
            }

            return result;
        }

        public List<cass.Index> GetIndexes(string columnFamily, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(List<cass.Index>);

            var returnList = new List<cass.Index>();

            foreach (var curKey in LocalContainer.data[this._keyspace + "." + columnFamily].Keys)
            {
                foreach (var curColumn in LocalContainer.data[this._keyspace + "." + columnFamily][curKey])
                {
                    var curIndex = new cass.Index
                    {
                        Name = curKey,
                        KeyValues = new Dictionary<string, string> {{curColumn.Key, curColumn.Value}}
                    };

                    returnList.Add(curIndex);
                }

            }

            return returnList;
        }

        public List<cass.Index> GetIndexes(string columnFamily, List<string> keys)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(List<cass.Index>);

            var returnList = new List<cass.Index>();

            foreach (var curKey in LocalContainer.data[this._keyspace + "." + columnFamily].Keys.Where(x => keys.Contains(x)))
            {
                var curIndex = new cass.Index();

                curIndex.Name = curKey;
                curIndex.KeyValues = new Dictionary<string, string>();

                foreach (var curColumn in LocalContainer.data[this._keyspace + "." + columnFamily][curKey])
                    curIndex.KeyValues.Add(curColumn.Key, curColumn.Value);

                returnList.Add(curIndex);
            }

            return returnList;
        }

        public Dictionary<string, string> GetTimeIndex(string columnFamily, string key, DateTime from, DateTime to)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return new Dictionary<string, string>();

            return LocalContainer.data[this._keyspace + "." + columnFamily][key]
                .Where(x => this.GetDateFromGuid(x.Key) >= from && this.GetDateFromGuid(x.Key) <= to)
                .ToDictionary(x => x.Key, x => x.Value);
        }

        public Dictionary<string, string> GetTimeIndex(string columnFamily, string key, DateTime from, DateTime to, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return new Dictionary<string, string>();

            if (limit > 0)
                return LocalContainer.data[this._keyspace + "." + columnFamily][key]
                    .Where(x => this.GetDateFromGuid(x.Key) >= from && this.GetDateFromGuid(x.Key) <= to)
                    .Take(limit)
                    .ToDictionary(x => x.Key, x => x.Value);
            else
                return LocalContainer.data[this._keyspace + "." + columnFamily][key]
                    .Where(x => this.GetDateFromGuid(x.Key) >= from && this.GetDateFromGuid(x.Key) <= to)
                    .ToDictionary(x => x.Key, x => x.Value);
        }

        public Dictionary<string, string> GetTimeIndex(string columnFamily, string key, string from, string to)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to));
        }

        public Dictionary<string, string> GetTimeIndex(string columnFamily, string key, string from, string to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to), limit);
        }

        public Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to));
        }

        public Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to), limit);
        }

        public Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return default(Dictionary<string, string>);

            if (limit > 0)
                return LocalContainer.data[this._keyspace + "." + columnFamily][key]
                    .Where(x => this.GetDateFromGuid(x.Key) >= this.GetDateFromGuid(from) && this.GetDateFromGuid(x.Key) <= this.GetDateFromGuid(to))
                    .Take(limit)
                    .OrderBy(x => x.Key)
                    .ToDictionary(x => x.Key, x => x.Value);
            else
                return LocalContainer.data[this._keyspace + "." + columnFamily][key]
                    .Where(x => this.GetDateFromGuid(x.Key) >= this.GetDateFromGuid(from) && this.GetDateFromGuid(x.Key) <= this.GetDateFromGuid(to))
                    .OrderBy(x => x.Key)
                    .ToDictionary(x => x.Key, x => x.Value);
        }

        public List<string> GetTimeIndexColumnNames(string columnFamily, string key, DateTime from, DateTime to)
        {
            return this.GetTimeIndex(columnFamily, key, from, to).Keys.ToList();
        }

        public List<string> GetTimeIndexColumnNames(string columnFamily, string key, DateTime from, DateTime to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, from, to).Keys.ToList();
        }

        public List<string> GetTimeIndexColumnNames(string columnFamily, string key, string from, string to)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to)).Keys.ToList();
        }

        public List<string> GetTimeIndexColumnNames(string columnFamily, string key, string from, string to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to), limit).Keys.ToList();
        }

        public List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, DateTime from, DateTime to)
        {
            return this.GetTimeIndex(columnFamily, key, from, to).Keys.ToList();
        }

        public List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, DateTime from, DateTime to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, from, to).Keys.ToList();
        }

        public List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to)).Keys.ToList();
        }

        public List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to), limit).Keys.ToList();
        }

        public List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to), limit).Keys.OrderBy(x => x).ToList();
        }

        public List<string> GetTimeIndexColumnValues(string columnFamily, string key, DateTime from, DateTime to)
        {
            return this.GetTimeIndex(columnFamily, key, from, to).Values.ToList();
        }

        public List<string> GetTimeIndexColumnValues(string columnFamily, string key, DateTime from, DateTime to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, from, to, limit).Values.ToList();
        }

        public List<string> GetTimeIndexColumnValues(string columnFamily, string key, string from, string to)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to)).Values.ToList();
        }

        public List<string> GetTimeIndexColumnValues(string columnFamily, string key, string from, string to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to), limit).Values.ToList();
        }

        public List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, DateTime from, DateTime to)
        {
            return this.GetTimeIndex(columnFamily, key, from, to).Values.ToList();
        }

        public List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, DateTime from, DateTime to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, from, to, limit).Values.ToList();
        }

        public List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to)).Values.ToList();
        }

        public List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to), limit).Values.ToList();
        }

        public List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to), limit).Values.OrderBy(x => x).ToList();
        }

        public List<string> GetTimeIndexsColumnValues(string columnFamily, string columnName, string keys, string from, string to, int limit, bool ascending)
        {
            var returnList = new List<string>();

            foreach (string key in keys.Split(','))
            {
                var curList = this.GetTimeIndex(columnFamily, key, this.GetDateFromGuid(from), this.GetDateFromGuid(to), limit).Values.OrderBy(x => x).ToList();
                returnList.AddRange(curList);
            }

            return returnList;
        }

        public List<T> GetAllRows(string columnFamily, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(List<T>);

            var returnList = new List<T>();

            foreach (var curKey in LocalContainer.data[this._keyspace + "." + columnFamily].Keys)
            {
                returnList.AddRange(
                    LocalContainer.data[this._keyspace + "." + columnFamily][curKey].Select(
                        curColumn => JSon.Deserialize<T>(curColumn.Value.ToString())));
            }

            return returnList;
        }

        public List<T> GetAllRows(string columnFamily, List<string> requestColumn, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(List<T>);
            var table = JToken.Parse(LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"]);
            var tableDef = JToken.Parse(table["Definition"].ToString());

            if (tableDef == null || !tableDef.Any()) return default(List<T>);
            string orderBy = tableDef["Order By"] != null ? tableDef["Order By"].ToString() : string.Empty;
            List<T> rows = JSon.Deserialize<List<T>>(table["Rows"].ToString());

            var response = !string.IsNullOrEmpty(orderBy)
                ? rows.OrderBy(orderBy) : rows;

            return response != null && response.Any() && limit > 0 ? response.Take(limit).ToList() : response.ToList();
        }

        public bool DeleteIndex(string columnFamily, string key)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return false;

            LocalContainer.data[this._keyspace + "." + columnFamily].Remove(key);

            return true;
        }

        public bool DeleteIndex(string columnFamily, string key, string indexKey)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return false;

            LocalContainer.data[this._keyspace + "." + columnFamily][key].Remove(indexKey);

            return true;
        }

        public bool DeleteIndex(string columnFamily, string columnName, string key, string indexKey)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return false;

            LocalContainer.data[this._keyspace + "." + columnFamily][key].Remove(indexKey);

            return true;
        }

        public bool DeleteTimeIndex(string columnFamily, string key, string indexKey)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return false;

            LocalContainer.data[this._keyspace + "." + columnFamily][key].Remove(indexKey);

            return true;
        }

        public bool DeleteTimeIndex(string columnFamily, string columnName, string key, string indexKey)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) || !LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey(indexKey))
                return false;

            LocalContainer.data[this._keyspace + "." + columnFamily][key].Remove(indexKey);

            return true;
        }

        public int Count(string columnFamily, string key)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return 0;

            return LocalContainer.data[this._keyspace + "." + columnFamily][key].Count;
        }

        public int Count(string columnFamily, string key, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily) || !LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key))
                return 0;

            if (limit > 0)
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Keys.Take(limit).Count();
            else
                return LocalContainer.data[this._keyspace + "." + columnFamily][key].Keys.Count();
        }

        static Dictionary<string, DateTime> dtguid = new Dictionary<string, DateTime>();
        public Guid GetTimeBasedGuid(DateTime dt)
        {
            var guid = Guid.NewGuid();

            dtguid.Add(guid.ToString(), dt);

            return guid;
        }

        public DateTime GetDateFromGuid(Guid guid)
        {
            return this.GetDateFromGuid(guid.ToString());
        }

        public DateTime GetDateFromGuid(string guid)
        {
            if (dtguid.ContainsKey(guid))
                return dtguid[guid];

            return Parser.ToDateTime(guid, default(DateTime));
        }

        public void Dispose()
        {
            LocalContainer.data = null;
            dtguid = null;
        }

        public T Get(string key, int retryCount, bool resetDBConnection)
        {
            throw new NotImplementedException();
        }

        public T Get(string columnFamily, string key, int retryCount, bool resetDBConnection)
        {
            throw new NotImplementedException();
        }

        public List<T> Get(List<string> keys, int retryCount, bool resetDBConnection)
        {
            throw new NotImplementedException();
        }

        public List<T> Get(string columnFamily, List<string> keys, int retryCount, bool resetDBConnection)
        {
            throw new NotImplementedException();
        }

        public bool AddIndexes(string columnFamily, List<cass.Index> indexes, uint timeToLive, int retryCount = 0)
        {
            throw new NotImplementedException();
        }

        public List<string> GetTimeIndexsColumnValues(string columnFamily, string columnName, List<string> keys, string from, string to, int limit, bool ascending)
        {
            throw new NotImplementedException();
        }

        public bool UpsertCounter(string columnFamily, List<CounterIndex> indexes)
        {
            string cf = this._keyspace + "." + columnFamily;
            foreach (var row in indexes)
                this.UpsertCounter(cf, row.KeyValues.First().Value.ToString(), row.Name, row.Value, row.Type);
            return true;
        }

        private bool UpsertCounter(string cf, string key, string counterColumnname, int counterValue, CounterType counterType)
        {
            System.Reflection.PropertyInfo propertyInfo = null;
            if (string.IsNullOrEmpty(cf) || string.IsNullOrEmpty(key))
                return false;

            var table = JToken.Parse(LocalContainer.data[cf]["key"]["json"]);
            List<T> tableRows = JSon.Deserialize<List<T>>(table["Rows"].ToString());
            Dictionary<string, object> definition = JSon.Deserialize<Dictionary<string, object>>(table["Definition"].ToString());

            string primaryKey = definition != null && definition.Any() && definition["Primary Key"] != null ? definition["Primary Key"].ToString() : string.Empty;

            if (string.IsNullOrEmpty(primaryKey))
                return false;
            Dictionary<string, string> whereColumnValues = new Dictionary<string,string>{{primaryKey, key}};
            string predicate = string.Join(" and ", whereColumnValues.Keys.Select((x, index) => string.Format("{0} == @{1}", x.Trim(), index)));
            T row = tableRows.Where(predicate, whereColumnValues.Values.ToArray()).FirstOrDefault();
            if (whereColumnValues.Any() && row != null)
                tableRows.Remove(row);

            if (row == null)
            {
                row = default(T);
                row = Activator.CreateInstance<T>();
                propertyInfo = row.GetType().GetProperty(primaryKey);
                propertyInfo.SetValue(row, key);
            }
            propertyInfo = row.GetType().GetProperty(counterColumnname);
            int counter = (int)(propertyInfo.GetValue(row, null));

            switch (counterType)
            {
                case CounterType.Increment:
                    counter = counter + counterValue;
                    break;
                case CounterType.Decrement:
                    counter = counter - counterValue;
                    break;
                case CounterType.Reset:
                    counter = 0;
                    break;
            }
            propertyInfo.SetValue(row, counter);
            tableRows.Add(row);
            Dictionary<string, object> result = new Dictionary<string, object>();
            result.Add("Definition", definition);
            result.Add("Rows", tableRows);
            LocalContainer.data[cf]["key"]["json"] = JSon.Serialize(result);

            return true;
        }

        public Dictionary<string, object> GetDataAsObject(string columnFamily, List<string> keys, string valueColumn)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return new Dictionary<string, object>();

            var result = new Dictionary<string, object>();
            foreach (string key in keys)
            {
                if ((LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) &&
                     LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey("json")))
                {
                    result.Add(key,
                        JSon.Deserialize<object>(LocalContainer.data[this._keyspace + "." + columnFamily][key]["json"]));
                }
            }
            return result;
        }

        public Dictionary<string, object> GetDataAsObject(string columnFamily, List<string> keys, string valueColumn, string keyColumn)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return new Dictionary<string, object>();

            var result = new Dictionary<string, object>();
            foreach (string key in keys)
            {
                if ((LocalContainer.data[this._keyspace + "." + columnFamily].ContainsKey(key) && LocalContainer.data[this._keyspace + "." + columnFamily][key].ContainsKey("json")))
                {
                    result.Add(key, JSon.Deserialize<object>(LocalContainer.data[this._keyspace + "." + columnFamily][key]["json"]));
                }
            }
            return result;
        }

        public bool DeleteIndex(string columnFamily, Dictionary<string, object> whereColumnValues)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return false;

            var table =  JToken.Parse(LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"]);
            Dictionary<string, object> result = new Dictionary<string, object> { { "Definition", table["Definition"] } };

            List<T> rows = JSon.Deserialize<List<T>>(table["Rows"].ToString());
            string predicate = string.Join(" and ", whereColumnValues.Keys.Select((x, index) => string.Format("{0} == @{1}", x.Trim(), index)));

            rows.RemoveAll(rows.Where(predicate, whereColumnValues.Values.ToArray()).Contains);
            result.Add("Rows" , rows);
            LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"] = JSon.Serialize(result);

            return true;
        }

        public List<T> GetIndexes(string columnFamily, Dictionary<string, string> whereColumnValues)
        {
            return this.GetIndexes(columnFamily, new List<string>(), whereColumnValues, 0);
        }

        public List<T> GetIndexes(string columnFamily, Dictionary<string, string> whereColumnValues, int limit)
        {
            return this.GetIndexes(columnFamily, new List<string>(), whereColumnValues, limit);
        }

        public List<T> GetIndexes(string columnFamily, List<string> returnColumn, Dictionary<string, string> whereColumnValues)
        {
            return this.GetIndexes(columnFamily, returnColumn, whereColumnValues, 0);
        }

        public List<T> GetIndexes(string columnFamily, List<string> returnColumn, Dictionary<string, string> whereColumnValues, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(List<T>);

            var table = JToken.Parse(LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"]);
            var tableDef = JToken.Parse(table["Definition"].ToString());

            if (tableDef == null || !tableDef.Any()) return default(List<T>);

            string orderBy = tableDef["Order By"] != null ? tableDef["Order By"].ToString() : string.Empty;
            List<T> rows = JSon.Deserialize<List<T>>(table["Rows"].ToString());
            string predicate = string.Join(" and ", whereColumnValues.Keys.Select((x, index) => string.Format("{0} == @{1}", x.Trim(), index)));

            var response = !string.IsNullOrEmpty(orderBy) ?
                rows.Where(predicate, whereColumnValues.Values.ToArray()).OrderBy(orderBy) :
                rows.Where(predicate, whereColumnValues.Values.ToArray());

            return response != null && response.Any() && limit > 0 ? response.Take(limit).ToList() : response.ToList();
        }

        public List<T> GetIndexesAsObject(string columnFamily, List<string> returnColumn, Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne, bool executeWithCLLocalQuorum)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(List<T>);

            var table = JToken.Parse(LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"]);
            var tableDef = JToken.Parse(table["Definition"].ToString());

            if (tableDef == null || !tableDef.Any()) return default(List<T>);

            string orderBy = tableDef["Order By"] != null ? tableDef["Order By"].ToString() : string.Empty;
            List<T> rows = JSon.Deserialize<List<T>>(table["Rows"].ToString());
            string predicate = string.Join(" and ", whereClause.Values.First().Keys.Select((x, index) => string.Format("{0} == @{1}", x.Trim(), index)));

            var response = !string.IsNullOrEmpty(orderBy) ?
                rows.Where(predicate, whereClause.Values.First().Values.ToArray()).OrderBy(orderBy) :
                rows.Where(predicate, whereClause.Values.First().Values.ToArray());

            return response != null && response.Any() && limit > 0 ? response.Take(limit).ToList() : response.ToList();
        }

        public List<T> ExecuteQueryForSOLR<T>(string columnFamily, List<string> requestColumn,
               Dictionary<int, Dictionary<string, object>> whereClause, int limit)
        {
            return ExecuteQueryCommon<T>(columnFamily, whereClause, limit);
        }

        public List<T> ExecuteQueryToGetOriginalDataRows(string columnFamily, List<string> requestColumn,
    Dictionary<int, Dictionary<string, object>> whereClause, int limit,
    bool executeWithCLOne = false, bool executeWithCLLocalQuorum = false, bool executeWithCLLocalOne = false)
        {
            return ExecuteQueryCommon<T>(columnFamily, whereClause, limit);
        }

        public List<T> ExecuteQuery<T>(string columnFamily, List<string> requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne = false,
            bool executeWithCLLocalQuorum = false)
        {
            return ExecuteQueryCommon<T>(columnFamily, whereClause, limit);
        }

        public List<T> ExecuteQuery<T>(string columnFamily, List<string> requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, string timeStampColumnName, DateTime start, DateTime end, int limit, bool executeWithCLOne = false,
            bool executeWithCLLocalQuorum = false)
        {
            return ExecuteQueryCommon<T>(columnFamily, requestColumn, whereClause, timeStampColumnName, start, end, limit, executeWithCLOne, executeWithCLLocalQuorum);
        }

        private List<T> ExecuteQueryCommon<T>(string columnFamily, Dictionary<int, Dictionary<string, object>> whereClause, int limit)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(List<T>);
            var table = JToken.Parse(LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"]);
            var tableDef = JToken.Parse(table["Definition"].ToString());

            if (tableDef == null || !tableDef.Any()) return default(List<T>);
            string orderBy = tableDef["Order By"] != null ? tableDef["Order By"].ToString() : string.Empty;
            List<T> rows = JSon.Deserialize<List<T>>(table["Rows"].ToString());
            string predicate = string.Join(" and ",
                whereClause.Values.First().Keys.Select((x, index) => string.Format("{0} == @{1}", x.Trim(), index)));

            var response = !string.IsNullOrEmpty(orderBy)
                ? rows.Where(predicate, whereClause.Values.First().Values.ToArray()).OrderBy(orderBy)
                : rows.Where(predicate, whereClause.Values.First().Values.ToArray());

            return response != null && response.Any() && limit > 0 ? response.Take(limit).ToList() : response.ToList();
        }

        private List<T> ExecuteQueryCommon<T>(string columnFamily, List<string> requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, string timeStampColumnName, DateTime start, DateTime end, int limit,
            bool executeWithCLOne = false, bool executeWithCLLocalQuorum = false, bool executeWithCLLocalOne = false)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return default(List<T>);
            var table = JToken.Parse(LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"]);
            var tableDef = JToken.Parse(table["Definition"].ToString());

            if (tableDef == null || !tableDef.Any()) return default(List<T>);
            string orderBy = tableDef["Order By"] != null ? tableDef["Order By"].ToString() : string.Empty;
            List<T> rows = JSon.Deserialize<List<T>>(table["Rows"].ToString());
            string predicate = string.Join(" and ",
                whereClause.Values.First().Keys.Select((x, index) => string.Format("{0} == @{1}", x.Trim(), index)));
            var response = !string.IsNullOrEmpty(orderBy)
                ? rows.Where(predicate, whereClause.Values.First().Values.ToArray()).OrderBy(orderBy)
                : rows.Where(predicate, whereClause.Values.First().Values.ToArray());

            return response != null && response.Any() && limit > 0 ? response.Take(limit).ToList() : response.ToList();
        }

        /// <summary>
        /// Operation to update column values for the specified column family
        /// </summary>
        /// <param name="columnFamily">The column family name</param>
        /// <param name="whereColumnValues">The where column values</param>
        /// <param name="setColumnValues">The set column values</param>
        /// <param name="autoTimeUUIDValues">The flags to update the timestamp or not,
        /// currently modification_timestamp column is supported, if the column is available for the column 
        /// family pass key as EnableAutoTimeUUID and value as true or false</param>
        /// <returns>Returns boolean value</returns>
        public bool Update(string columnFamily, Dictionary<string, object> whereColumnValues, Dictionary<string, object> setColumnValues,
            Dictionary<string,bool> autoTimeUUIDValues)
        {
            if (!LocalContainer.data.ContainsKey(this._keyspace + "." + columnFamily))
                return false;

            var table =  JToken.Parse(LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"]);
            Dictionary<string, object> result = new Dictionary<string, object> { { "Definition", table["Definition"] } };

            List<Dictionary<string, object>> rows = JSon.Deserialize<List<Dictionary<string, object>>>(table["Rows"].ToString());
            
            bool matchFound = false;
            foreach (var row in rows)
            {
                foreach (var col in whereColumnValues)
                {
                    if (row.ContainsKey(col.Key) && row[col.Key].ToString() == col.Value.ToString())
                    {
                        matchFound = true;
                    }
                    else
                    {
                        matchFound = false;
                        break;
                    }
                }
                if (!matchFound) continue;
                foreach (var col in setColumnValues)
                {
                    if (row.ContainsKey(col.Key))
                    {
                        row[col.Key] = col.Value;
                    }
                }
                if (autoTimeUUIDValues != null && autoTimeUUIDValues.ContainsKey("EnableAutoTimeUUID") && 
                    autoTimeUUIDValues["EnableAutoTimeUUID"] && row.ContainsKey("modification_timestamp"))
                    row["modification_timestamp"] = this.GetTimeBasedGuid(DateTime.UtcNow);
            }

            dynamic updatedTable = new ExpandoObject();
            updatedTable.Rows = JSon.Serialize(rows);
            updatedTable.Definition = table["Definition"];

            LocalContainer.data[this._keyspace + "." + columnFamily]["key"]["json"] = JSon.Serialize(updatedTable);

            return true;
        }
    }
}