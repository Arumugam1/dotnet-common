using System;
using System.Collections.Generic;
using automation.components.operations.v1;
using automation.components.data.v1.Config;
using automation.components.data.v1.Entities;
using automation.components.data.v1.CustomTypes;
using automation.components.data.v1.ElasticSearch;

namespace automation.components.data.v1.Local.ElasticSearch
{
    public class Communicator<T> : ICommunicator<T> where T : class
    {
        #region Private Members

        internal string _indexName { get; set; }
        internal string _typeName { get; set; }
        internal const int MaxRecordCount = 1000000;

        #endregion

        #region Constructors

        public Communicator() { }

        public Communicator(string indexName, string typeName)
        {
            this._indexName = indexName;
            this._typeName = typeName;
        }

        #endregion

        #region ICommunicator Members

        public virtual bool Upsert(string key, T item)
        {
            try
            {
                var cf = _indexName + "-" + _typeName;
                if (string.IsNullOrEmpty(key))
                    return false;

                if (!LocalContainer.data.ContainsKey(cf))
                    LocalContainer.data.Add(cf, new Dictionary<string, Dictionary<string, string>>());

                if (!LocalContainer.data[cf].ContainsKey(key))
                    LocalContainer.data[cf].Add(key, new Dictionary<string, string>());

                if (!LocalContainer.data[cf][key].ContainsKey("json"))
                    LocalContainer.data[cf][key].Add("json", JSon.Serialize<T>(item));
                else
                    LocalContainer.data[cf][key]["json"] = JSon.Serialize<T>(item);

                return true;
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "Upsert",
                        new object[] { _indexName, _typeName, key, item },
                        ex);

                return false;
            }
        }

        public virtual bool Upsert(string keyField, List<T> items)
        {
            throw new NotImplementedException();
        }

        public virtual bool Delete(string key)
        {
            var cf = _indexName + "-" + _typeName;
            // throw new NotImplementedException();
            if (!LocalContainer.data.ContainsKey(cf) || !LocalContainer.data[cf].ContainsKey(key) || !LocalContainer.data[cf][key].ContainsKey("json"))
                return true;

            return LocalContainer.data[cf].Remove(key);
        }

        public virtual T GetObjectByKey(string key)
        {
            throw new NotImplementedException();
        }

        public virtual T GetObjectByKey(string key, string baseFilter)
        {
            throw new NotImplementedException();
        }

        public virtual Pagination<T> GetPagedObjects(string query, string filter, Pagination<T> pagination)
        {
            throw new NotImplementedException();
        }

        public virtual Pagination<T> GetPagedObjects(string query, string filter, string sortQuery, Pagination<T> pagination)
        {
            throw new NotImplementedException();
        }

        public virtual Pagination<T> GetPagedObjects(string query, string filter, string pageQuery, string sortQuery, Pagination<T> pagination)
        {
            throw new NotImplementedException();
        }

        public virtual ElasticResponse<T> ExecuteQuery(string query)
        {
            throw new NotImplementedException();
        }

        public virtual List<T> ExecuteQuery(T entity, string expression)
        {
            throw new NotImplementedException();
        }

        public virtual List<T> ExecuteQueryWithBatchSize(T entity, string expression, int batchSize)
        {
            throw new NotImplementedException();
        }

        public virtual Pagination<T> GetPagedObjects(T entity, string expression, Pagination<T> pagination)
        {
            throw new NotImplementedException();
        }

        #region Get Objects by values

        public virtual List<T> GetObjectsByValue(string field, string value)
        {
            return GetObjectsByValue(field, value, MaxRecordCount);
        }

        public virtual List<T> GetObjectsByValue(string field, string value, int size)
        {
            return GetObjectsByValue(field, value, null, size);
        }

        public virtual List<T> GetObjectsByValue(string field, string value, string baseFilter)
        {
            return GetObjectsByValue(field, value, baseFilter, MaxRecordCount);
        }

        public virtual List<T> GetObjectsByValue(string field, string value, string baseFilter, int size)
        {
            throw new NotImplementedException();
        }
        #endregion

        public virtual List<T> GetObjectsByValues(string field, List<string> values)
        {
            return GetObjectsByValues(field, values, MaxRecordCount);
        }

        public virtual List<T> GetObjectsByValues(string field, List<string> values, int size)
        {
            return GetObjectsByValues(field, values, null, size);
        }


        public virtual List<T> GetObjectsByValues(string field, List<string> values, string baseFilter)
        {
            return GetObjectsByValues(field, values, baseFilter, MaxRecordCount);
        }

        public virtual List<T> GetObjectsByValues(string field, List<string> values, string baseFilter, int size)
        {
            throw new NotImplementedException();
        }

        public virtual List<T> GetObjectsByValues(List<string> fields, List<string> values)
        {
            return GetObjectsByValues(fields, values, MaxRecordCount);
        }

        public virtual List<T> GetObjectsByValues(List<string> fields, List<string> values, int size)
        {
            return GetObjectsByValues(fields, values, null, size);
        }

        public virtual List<T> GetObjectsByValues(List<string> fields, List<string> values, string baseFilter)
        {
            return GetObjectsByValues(fields, values, baseFilter, MaxRecordCount);
        }

        public virtual List<T> GetObjectsByValues(List<string> fields, List<string> values, string baseFilter, int size)
        {
            throw new NotImplementedException();

        }

        public virtual T GetObjectByValue(string field, string key)
        {
            return GetObjectByValue(field, key, null);
        }
        public virtual T GetObjectByValue(string field, string key, string baseFilter)
        {
            throw new NotImplementedException();
        }

        #endregion


        private static readonly string elasticSearchURI = (Manager.GetApplicationConfigValue("ElasticSearch", "AutomationServices.EventAggregation.Providers.Cassandra")) == string.Empty ? "EventAggregator" : (Manager.GetApplicationConfigValue("ElasticSearch", "AutomationServices.EventAggregation.Providers.Cassandra"));

        public static bool CreateIndex(string index, string type, string key, string payload)
        {
            throw new NotImplementedException();
        }

        public static bool GetOnKey(string index, string type, string key)
        {
            throw new NotImplementedException();
        }

        public static string GetClientURL()
        {
            throw new NotImplementedException();
        }

        public virtual void CreateIndex(T entity, string uniqueKey)
        {
            throw new NotImplementedException();
        }

        public virtual void UpsertIndex(T entity, string uniqueKey)
        {
            throw new NotImplementedException();
        }

        public virtual bool UpsertIndex(string payload, string uniqueKey)
        {
            try
            {
                var cf = _indexName + "-" + _typeName;
                if (string.IsNullOrEmpty(uniqueKey))
                    throw new Exception("Invalid Key");

                if (!LocalContainer.data.ContainsKey(cf))
                    LocalContainer.data.Add(cf, new Dictionary<string, Dictionary<string, string>>());

                if (!LocalContainer.data[cf].ContainsKey(uniqueKey))
                    LocalContainer.data[cf].Add(uniqueKey, new Dictionary<string, string>());

                if (!LocalContainer.data[cf][uniqueKey].ContainsKey("json"))
                    LocalContainer.data[cf][uniqueKey].Add("json", payload);
                else
                    LocalContainer.data[cf][uniqueKey]["json"] = payload;

                return true;
            }
            catch (Exception ex)
            {
                // To-Do: We can replace this with new Logging when it implement to this repo
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "Upsert",
                        new object[] { _indexName, _typeName, uniqueKey, payload },
                        ex);

                return false;
            }
        }

        public virtual void DeleteIndex(T entity, string uniqueKey)
        {
            throw new NotImplementedException();
        }

        public virtual bool UpdateIndex(string payload, string uniqueKey, int requestTimeout)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// execute the query for the given index
        /// </summary>
        /// <param name="entity">name of the entity</param>
        /// <param name="expression">query</param>
        /// <param name="indexName">Name of the index</param>
        /// <param name="typeName">Index Type</param>
        /// <returns>list of entity</returns>
        public virtual List<T> ExecuteQuery(T entity, string expression, string indexName, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Execute the query with batch size
        /// </summary>
        /// <param name="entity">entity</param>
        /// <param name="expression">expression</param>
        /// <param name="batchSize">No of records to be return</param>
        /// <param name="indexName">Name of the index</param>
        /// <param name="typeName">Index Type</param>
        /// <returns>list of entity</returns>
        public virtual List<T> ExecuteQueryWithBatchSize(T entity, string expression, int batchSize, string indexName, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Create a record for the key
        /// </summary>
        /// <param name="entity">entity</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="indexName">name of the index</param>
        /// <param name="typeName">Index Type</param>
        /// <returns>true or false</returns>
        public virtual bool CreateIndex(T entity, string uniqueKey, string indexName, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Create a record for the key
        /// </summary>
        /// <param name="entity">entity</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="indexName">Name of the index</param>
        /// <param name="typeName">Index Type</param>
        /// <returns>true or false</returns>
        public virtual bool UpsertIndex(T entity, string uniqueKey, string indexName, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Create a record for the key
        /// </summary>
        /// <param name="payload">payload</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="indexName">name of the index</param>
        /// <param name="typeName">Index Type</param>
        /// <returns>true or false</returns>
        public virtual bool UpsertIndex(string payload, string uniqueKey, string indexName, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// update index for the given key
        /// </summary>
        /// <param name="payload">payload</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="requestTimeout">Request Timeout</param>
        /// <param name="indexName">Name of the index</param>
        /// <param name="typeName">Index Type</param>
        /// <returns>true or false</returns>
        public virtual bool UpdateIndex(string payload, string uniqueKey, int requestTimeout, string indexName, string typeName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// delete index for the given key
        /// </summary>
        /// <param name="entity">entity</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="indexName">Name of the index</param>
        /// <param name="typeName">Index Type</param>
        public void DeleteIndex(T entity, string uniqueKey, string indexName, string typeName)
        {
            throw new NotImplementedException();
        }
    }
}
