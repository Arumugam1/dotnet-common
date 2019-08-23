using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using automation.components.operations.v1;
using automation.components.data.v1.Config;
using System.Net;
using System.Web;
using Elasticsearch.Net;
using automation.components.data.v1.Entities;
using automation.components.data.v1.CustomTypes;
using Microsoft.Data.Schema.ScriptDom;
using Microsoft.Data.Schema.ScriptDom.Sql;
using System.Text.RegularExpressions;
using Nest;
using OPSManager = automation.components.operations.v1;
using automation.components.operations.v1.JSonExtensions;
using automation.components.operations.v1.JSonExtensions.Converters;
using System.Collections.Specialized;
using automation.components.data.v1.Graph;
using automation.components.data.v1.Graph.Providers;

namespace automation.components.data.v1.ElasticSearch
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum LogicalOperator
    {
        AND,
        OR,
        NONE
    }

    [JsonConverter(typeof(StringEnumConverter))]
    public enum ConnectionType
    {
        Create,
        Delete,
        Search,
        Upsert
    }

    public interface ICommunicator<T>  where T : class
    {
        [Obsolete("Upsert is deprecated, please use UpsertIndex instead.")]
        bool Upsert(string key, T item);
        [Obsolete("Upsert is deprecated, please use UpsertIndex instead.")]
        bool Upsert(string keyField, List<T> items);

        [Obsolete("Delete is deprecated, please use DeleteIndex instead.")]
        bool Delete(string key);
        [Obsolete("ExecuteQuery is deprecated, please use ExecuteQuery(T entity, string expression) instead.")]
        ElasticResponse<T> ExecuteQuery(string query);

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        Pagination<T> GetPagedObjects(string query, string filter, Pagination<T> pagination);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        Pagination<T> GetPagedObjects(string query, string filter, string sortQuery, Pagination<T> pagination);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        Pagination<T> GetPagedObjects(string query, string filter, string pageQuery, string sortQuery, Pagination<T> pagination);

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        T GetObjectByKey(string key);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        T GetObjectByKey(string key, string baseFilter);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        T GetObjectByValue(string field, string key);

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValue(string field, string value);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValue(string field, string value, int size);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValue(string field, string value, string baseFilter);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValue(string field, string value, string baseFilter, int size);

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValues(string field, List<string> values);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValues(string field, List<string> values, int size);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValues(string field, List<string> values, string baseFilter);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValues(string field, List<string> values, string baseFilter, int size);

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValues(List<string> fields, List<string> values);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValues(List<string> field, List<string> values, int size);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValues(List<string> fields, List<string> values, string baseFilter);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        List<T> GetObjectsByValues(List<string> field, List<string> values, string baseFilter, int size);
        List<T> ExecuteQuery(T entity, string expression);
        List<T> ExecuteQuery(T entity, string expression, string indexName, string typeName);
        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]


        List<T> ExecuteQueryWithBatchSize(T entity, string expression, int batchSize);
        List<T> ExecuteQueryWithBatchSize(T entity, string expression, int batchSize, string indexName, string typeName);
        Pagination<T> GetPagedObjects(T entity, string expression, Pagination<T> pagination);
        void CreateIndex(T entity, string uniqueKey);
        bool CreateIndex(T entity, string uniqueKey, string indexName, string typeName);
        void UpsertIndex(T entity, string uniqueKey);
        bool UpsertIndex(string payload, string uniqueKey);
        bool UpsertIndex(T entity, string uniqueKey, string indexName, string typeName);
        bool UpsertIndex(string payload, string uniqueKey, string indexName, string typeName);
        void DeleteIndex(T entity, string uniqueKey);
        void DeleteIndex(T entity, string uniqueKey, string indexName, string typeName);
        bool UpdateIndex(string payload, string uniqueKey, int requestTimeout);
        bool UpdateIndex(string payload, string uniqueKey, int requestTimeout, string indexName, string typeName);
    }

    public abstract class Communicator<T> : ICommunicator<T> where T : class
    {
        #region Private Members
        private const string AuthToken = "x-auth-token";
        private const string ContentType = "application/json";
        private const string ClassName = "automation.components.data.v1.ElasticSearch.Communicator";
        private const string GET = "get";
        private const string POST = "post";
        private const string BULK = "bulk";
        private const string DELETE = "delete";

        internal string _indexName { get; set; }
        internal string _typeName { get; set; }

        internal int MaxRecordCount = operations.v1.Parser.ToInt(Manager.GetApplicationConfigValue("MaxRecordCount", "ElasticSearch"), 1000000);

        #endregion

        #region Public Members

        private static bool IsAuthEnabled
        {
            get
            {
                return operations.v1.Parser.ToBoolean(Manager.GetApplicationConfigValue("ElasticSearch.Auth.Enabled", "FeatureFlag"), false);
            }
        }

        private static string GetAuthToken
        {
            get
            {
                return Manager.GetApplicationConfigValue("session", "globalauth.2.0.us");
            }
        }

        public string ElasticURL
        {
            get
            {
                return Manager.GetApplicationConfigValue("ElasticSearch", "AutomationServices.EventAggregation.Providers.Cassandra");
            }
        }

        private bool IsNestSearchEnabled
        {
            get
            {
                return operations.v1.Parser.ToBoolean(Manager.GetApplicationConfigValue("ElasticSearch.NestSearch.Enabled", "FeatureFlag"), false);
            }
        }

        private static int defaultBatchSize;
        /// <summary>
        /// Default Batch Size
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        protected static int DefaultBatchSize
        {
            get
            {
                if (defaultBatchSize <= 0)
                    defaultBatchSize = operations.v1.Parser.ToInt(Manager.GetApplicationConfigValue("DefaultBatchSize", "ElasticSearch.ExecuteQuery"), 2000);
                return defaultBatchSize;
            }
            set { throw new NotImplementedException(); }
        }

        public ElasticLowLevelClient Client { get; set; }

        private const string logicaloperator_pattern = @"(?<=^|\A|(\bAND\b|\bOR\b))(?:[^']|'(?:[^']|'{2})+')*?(?=(\bAND\b|\bOR\b)|$|\Z)";
        private const string condition_pattern = @"(>=)|(<=)|(!=)|(=)|(>)|(<)|(\blike\b)|(\bcontains\b)|(\bnot contains\b)|(\bbegins with\b)|(\bends with\b)|(\bbetween\b)";

        private ElasticClient ElasticClientProvider { get; set; }
        private const string ConditionWithEmptySpace = "#N/A";
        private static bool forceSecurityProtocol = automation.components.operations.v1.Parser.ToBoolean(Manager.GetApplicationConfigValue("ForceTls11AndUp", "Feature_Flag"), false);

        #endregion

        #region Producted Members

        protected Communicator(string indexName, string typeName)
        {
            this._indexName = indexName;
            this._typeName = typeName;
            var config = new ConnectionConfiguration(new Uri(ElasticURL));
            if (IsAuthEnabled)
            {
                config.GlobalHeaders(new NameValueCollection
                {
                    { AuthToken, GetAuthToken }
                });
            }
            Client = new ElasticLowLevelClient(config);
            SetSecurityProtocol();
        }

        private static void SetSecurityProtocol()
        {
            if (forceSecurityProtocol)
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
            }
            else
            {
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
            }
        }

        private void OpenConnection(ConnectionType connectionType)
        {
            string uri = string.Format("{0}{1}", ElasticURL,
                    Enum.Equals(connectionType, ConnectionType.Search) ? string.Format("{0}/", _indexName.ToLower()) : string.Empty);
            var settings = new ConnectionSettings(new Uri(uri));
            if (IsAuthEnabled)
            {
                settings.GlobalHeaders(new NameValueCollection
                {
                    { AuthToken, GetAuthToken }
                });
            }

            settings.DefaultFieldNameInferrer(p => p);
            ElasticClientProvider = new ElasticClient(settings);
        }

        #endregion


        #region ICommunicator Members

        [Obsolete("Upsert is deprecated, please use UpsertIndex instead.")]
        public virtual bool Upsert(string key, T item)
        {
            try
            {
                bool status = false;
                if (this.IsNestSearchEnabled)
                {
                    status = this.PostDatatoElasticSearchServer(item, key);
                }
                else
                {
                    var indexResponse = this.Client.Index<dynamic>(_indexName, _typeName, key, item);
                    status = indexResponse.Success;
                }

                return status;
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

        [Obsolete("Upsert is deprecated, please use UpsertIndex instead.")]
        public virtual bool Upsert(string keyField, List<T> items)
        {
            try
            {
                if (items == null || items.Count <= 0)
                {
                    return false;
                }
                List<object> bulkData = new List<object>();

                //Find key method/field of the source entity
                System.Reflection.MethodInfo keyMethod;
                System.Reflection.PropertyInfo keyProperty = items[0].GetType().GetProperty(keyField);
                if (keyProperty == null)
                {
                    keyMethod = items[0].GetType().GetMethod(keyField);
                    if (keyMethod == null)
                    {
                        return false;
                    }
                    else 
                    {
                        //Build the bulk data list
                        foreach (var item in items)
                        {
                            bulkData.Add(new { index = new { _id = keyMethod.Invoke(item, null) } });
                            bulkData.Add(item);
                        }
                    }
                }
                else
                {
                    //Build the bulk data list
                    foreach (var item in items)
                    {
                        bulkData.Add(new { index = new { _id = keyProperty.GetValue(item, null) } });
                        bulkData.Add(item);
                    }
                }

                bool status = false;
                if (this.IsNestSearchEnabled)
                {
                    status = this.BulkUpdates(bulkData.ToArray());
                }
                else
                {
                    var bulkResponse = this.Client.Bulk<dynamic>(_indexName, _typeName, bulkData.ToArray());
                    status = bulkResponse.Success;
                }

                return status;
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "Upsert",
                        new object[] { _indexName, _typeName, keyField, items },
                        ex);

                return false;
            }

        }

        [Obsolete("Delete is deprecated, please use DeleteIndex instead.")]
        public virtual bool Delete(string key)
        {
            try
            {
                bool status = false;
                if (this.IsNestSearchEnabled)
                {
                    status = this.DeleteIndex(key);
                }
                else
                {
                    var deleteResponse = this.Client.Delete<dynamic>(_indexName, _typeName, key);
                    status = deleteResponse.Success;
                }

                return status;
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "Delete",
                        new object[] { _indexName, _typeName, key },
                        ex);

                return false;
            }
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual T GetObjectByKey(string key)
        {
            return GetObjectByKey(key, null);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual T GetObjectByKey(string key, string baseFilter)
        {
            try
            {
                string filter = "{\"term\":{\"_id\":\"" + key + "\"}}";

                if (!string.IsNullOrEmpty(baseFilter))
                {
                    filter = "{\"bool\":{\"must\":[" + filter + "," + baseFilter + "]}}";
                }

                if (this.IsNestSearchEnabled)
                {
                    return this.SearchObjectByValue(filter);
                }
                else
                {
                    string query = "{\"filter\":" + filter + "}";

                    var convertedResponse = ExecuteQuery(query);
                    if (convertedResponse.hits.total > 0)
                    {
                        return convertedResponse.GetDocuments()[0];
                    }
                    else
                    {
                        return default(T);
                    }
                }
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "GetObjectByKey",
                        new object[] { _indexName, _typeName, key },
                        ex);

                return default(T);
            }
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual Pagination<T> GetPagedObjects(string query, string filter, Pagination<T> pagination)
        {
            //create pagination query
            string pageQuery = "\"size\":" + pagination.PageSize + ",\"from\": " + pagination.From,
                sortQuery = string.Empty;

            //create sorting query
            if (!string.IsNullOrEmpty(pagination.SortField))
            {
                sortQuery = "\"sort\": " + JSon.Serialize(GetSortFields(pagination.SortField, pagination.Sort.ToString()));
            }

            return GetPagedObjects(query, filter, pageQuery, sortQuery, pagination);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual Pagination<T> GetPagedObjects(string query, string filter, string sortQuery, Pagination<T> pagination)
        {
            //create pagination query
            string pageQuery = "\"size\":" + pagination.PageSize + ",\"from\": " + pagination.From;


            return GetPagedObjects(query, filter, pageQuery, sortQuery, pagination);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual Pagination<T> GetPagedObjects(string query, string filter, string pageQuery, string sortQuery, Pagination<T> pagination)
        {
            if (this.IsNestSearchEnabled)
            {
                return this.SearchPagedObjects(query, filter, pageQuery, sortQuery, pagination);
            }
            else
            {
                var filteredQuery = string.Empty;
                pageQuery = string.IsNullOrEmpty(pageQuery) ? "" : "," + pageQuery;
                sortQuery = string.IsNullOrEmpty(sortQuery) ? "" : "," + sortQuery;

                query = string.IsNullOrEmpty(query) ? string.Empty : "\"query\":" + query;
                filter = string.IsNullOrEmpty(filter) ? string.Empty : (string.IsNullOrEmpty(query) ? string.Empty : ", ") + "\"filter\":" + filter;

                //created fitlered query or merge the query & filters
                if (string.IsNullOrEmpty(query) || string.IsNullOrEmpty(filter))
                {
                    filteredQuery = query + filter;
                }
                else
                {
                    filteredQuery = "\"query\":{\"filtered\":{ " + query + filter + "}} ";
                }

                string searchQuery = "{" + filteredQuery + pageQuery + sortQuery + "}";

                var convertedResponse = ExecuteQuery(searchQuery);
                if (convertedResponse != null)
                {
                    pagination.ResultSet = new Result<T>() { TotalRecords = convertedResponse.hits.total, ResultSet = convertedResponse.GetDocuments() };
                }

                return pagination;
            }
        }

        private Pagination<T> SearchPagedObjects(string query, string filter, string pageQuery, string sortQuery, Pagination<T> pagination)
        {
            pageQuery = string.IsNullOrEmpty(pageQuery) ? string.Empty : "," + pageQuery;
            sortQuery = string.IsNullOrEmpty(sortQuery) ? string.Empty : "," + sortQuery;

            query = string.IsNullOrEmpty(query) ? string.Empty : @"""must"":" + query;
            filter = string.IsNullOrEmpty(filter) ? string.Empty : (string.IsNullOrEmpty(query) ? string.Empty : ", ") + @"""filter"":" + filter;

            string filteredQuery = @"""query"":{""bool"":{ " + query + filter + "}} ";

            string searchQuery = "{" + string.Format("{0}{1}{2}", filteredQuery, pageQuery, sortQuery) + "}";

            var response = this.SearchByQuery(searchQuery);
            if (response != null && response.hits != null)
            {
                pagination.ResultSet = new Result<T>()
                {
                    TotalRecords = response.hits.total,
                    ResultSet = response.GetDocuments()
                };
            }

            return pagination;
        }

        [Obsolete("ExecuteQuery is deprecated, please use ExecuteQuery(T entity, string expression) instead.")]
        public virtual ElasticResponse<T> ExecuteQuery(string query)
        {

            try
            {
                if (this.IsNestSearchEnabled)
                {
                    return this.SearchByQuery(query);
                }
                else
                {
                    //search elastic search index with given query
                    var elasticResponse = this.Client.Search<string>(_indexName, _typeName, query);

                    if (elasticResponse.Success)
                    {
                        return JSon.Deserialize<ElasticResponse<T>>(elasticResponse.Body);
                    }
                    else
                    {
                        throw new Exception("Unexpected error occured in ElasticSearch. \n Detailied Information : " + elasticResponse.ServerError);
                    }
                }
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "ExecuteQuery",
                        new object[] { _indexName, _typeName, query },
                        ex);

                throw ex;
            }

        }

        #region Get Objects by values

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValue(string field, string value)
        {
            return GetObjectsByValue(field, value, MaxRecordCount);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValue(string field, string value, int size)
        {
            return GetObjectsByValue(field, value, null, size);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValue(string field, string value, string baseFilter)
        {
            return GetObjectsByValue(field, value, baseFilter, MaxRecordCount);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValue(string field, string value, string baseFilter, int size)
        {
            if (size > MaxRecordCount)
            {
                size = MaxRecordCount;
            }

            try
            {
                if (this.IsNestSearchEnabled)
                {
                    return this.SearchObjectsByValue(field, value, baseFilter, size);
                }

                string filter = "{\"term\":{\"" + field + "\":\"" + value + "\"}}";

                if (!string.IsNullOrEmpty(baseFilter))
                {
                    filter = "{\"and\":[" + baseFilter + ", " + filter + "]}";
                }

                string query = "{\"size\": " + size + ", \"filter\":" + filter + "}";

                //search elastic search index with given query
                var searchElasticResponse = ExecuteQuery(query);
                if (searchElasticResponse != null)
                {
                    return searchElasticResponse.GetDocuments();
                }
                else
                {
                    return default(List<T>);
                }
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "GetObjectsByValue",
                        new object[] { _indexName, _typeName, field, value, size },
                        ex);

                return default(List<T>);
            }
        }

        private List<T> SearchObjectsByValue(string field, string value, string baseFilter, int size)
        {
            string filter = @"{""term"":{""" + field + @""":""" + value + @"""}}";
            return this.PreparSearchObject(filter, baseFilter, size);
        }

        private List<T> SearchObjectsByValues(string field, List<string> values, string baseFilter, int size)
        {
            string filter = @"{""terms"":{""" + field + @""":" + JSon.Serialize<List<string>>(values) + "}}";
            return this.PreparSearchObject(filter, baseFilter, size);
        }

        private List<T> PreparSearchObject(string filter, string baseFilter, int size)
        {
            if (!string.IsNullOrEmpty(baseFilter))
            {
                filter = @"{""bool"":{""must"":[" + baseFilter + ", " + filter + "]}}";
            }

            string query = @"{""size"": " + size + @", ""query"":" + filter + "}";
            var response = this.SearchByQuery(query);
            return response != null && response.hits != null ?
                response.GetDocuments() : default(List<T>);
        }
        #endregion

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValues(string field, List<string> values)
        {
            return GetObjectsByValues(field, values, MaxRecordCount);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValues(string field, List<string> values, int size)
        {
            return GetObjectsByValues(field, values, null, size);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValues(string field, List<string> values, string baseFilter)
        {
            return GetObjectsByValues(field, values, baseFilter, MaxRecordCount);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValues(string field, List<string> values, string baseFilter, int size)
        {
            if (size > MaxRecordCount)
            {
                size = MaxRecordCount;
            }

            try
            {
                if (this.IsNestSearchEnabled)
                {
                    return this.SearchObjectsByValues(field, values, baseFilter, size);
                }

                string filter = "{\"terms\":{\"" + field + "\":" + JSon.Serialize<List<string>>(values) + "}}";

                if (!string.IsNullOrEmpty(baseFilter))
                {
                    filter = "{\"and\":[" + baseFilter + ", " + filter + "]}";
                }

                string query = "{\"size\": " + size + ", \"filter\":" + filter + "}";

                //search elastic search index with given query
                var searchElasticResponse = ExecuteQuery(query);
                if (searchElasticResponse != null)
                {
                    return searchElasticResponse.GetDocuments();
                }
                else
                {
                    return default(List<T>);
                }
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "GetObjectsByValues",
                        new object[] { _indexName, _typeName, field, values, size },
                        ex);

                return default(List<T>);
            }
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValues(List<string> fields, List<string> values)
        {
            return GetObjectsByValues(fields, values, MaxRecordCount);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValues(List<string> fields, List<string> values, int size)
        {
            return GetObjectsByValues(fields, values, null, size);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValues(List<string> fields, List<string> values, string baseFilter)
        {
            return GetObjectsByValues(fields, values, baseFilter, MaxRecordCount);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual List<T> GetObjectsByValues(List<string> fields, List<string> values, string baseFilter, int size)
        {
            if (size > MaxRecordCount)
            {
                size = MaxRecordCount;
            }

            try
            {
                if (this.IsNestSearchEnabled)
                {
                    return this.SearchObjectsByValues(fields, values, baseFilter, size);
                }

                string filter = string.Empty;
                var valuesText = JSon.Serialize<List<string>>(values);
                List<string> termsQuery = new List<string>();
                foreach (var field in fields)
                {
                    termsQuery.Add("{\"terms\":{\"" + field + "\":" + valuesText + "}}");
                }

                filter = "{\"or\":{\"filters\":[" + string.Join(",", termsQuery) + "]}}";

                if (!string.IsNullOrEmpty(baseFilter))
                {
                    filter = "{\"and\":[" + baseFilter + ", " + filter + "]}";
                }

                string query = "{\"size\": " + size + ", \"filter\":" + filter + "}";

                //search elastic search index with given query
                var searchElasticResponse = ExecuteQuery(query);
                if (searchElasticResponse != null)
                {
                    return searchElasticResponse.GetDocuments();
                }
                else
                {
                    return default(List<T>);
                }
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "GetObjectsByValues",
                        new object[] { _indexName, _typeName, fields, values, size },
                        ex);

                return default(List<T>);
            }

        }

        private List<T> SearchObjectsByValues(List<string> fields, List<string> values, string baseFilter, int size)
        {
            var valuesText = JSon.Serialize<List<string>>(values);
            List<string> termsQuery = new List<string>();
            foreach (var field in fields)
            {
                termsQuery.Add(@"{""terms"":{""" + field + @""":" + valuesText + "}}");
            }

            string filter = @"{""bool"":{""should"":[" + string.Join(",", termsQuery) + "]}}";

            if (!string.IsNullOrEmpty(baseFilter))
            {
                filter = @"{""bool"":{""must"":[" + baseFilter + ", " + filter + "]}}";
            }

            string query = @"{""size"": " + size + @", ""query"":" + filter + "}";

            var response = this.SearchByQuery(query);
            return response != null && response.hits != null ?
                response.GetDocuments() : default(List<T>);
        }

        [Obsolete("GetPagedObjects is deprecated, please use GetPagedObjects(T entity, string expression, Pagination<T> pagination) instead.")]
        public virtual T GetObjectByValue(string field, string key) {
            return GetObjectByValue(field, key, null);
        }
        public virtual T GetObjectByValue(string field, string key, string baseFilter)
        {
            try
            {
                string filter = "{\"term\":{\"" + field + "\":\"" + key + "\"}}";

                if (!string.IsNullOrEmpty(baseFilter))
                {
                    filter = "{\"bool\":{\"must\":[" + filter + "," + baseFilter + "]}}";
                }

                if (this.IsNestSearchEnabled)
                {
                    return this.SearchObjectByValue(filter);
                }
                else
                {
                    string query = "{\"filter\":" + filter + "}";

                    //string query = "{\"filter\":{\"term\":{\"" + field + "\":\"" + key + "\"}}}";
                    var convertedResponse = ExecuteQuery(query);

                    if (convertedResponse.hits.total > 0)
                    {
                        return convertedResponse.GetDocuments()[0];
                    }
                    else
                    {
                        return default(T);
                    }
                }
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.ElasticSearch.Communicator<T>",
                        "GetObjectByValue",
                        new object[] { _indexName, _typeName, key },
                        ex);

                return default(T);
            }
        }

        private T SearchObjectByValue(string filter)
        {
            string query = @"{""query"":" + filter + "}";
            var response = this.SearchByQuery(query);
            return response != null && response.hits != null && response.hits.total > 0 ?
                response.GetDocuments()[0] : default(T);
        }

        #endregion

        private static readonly string ElasticSearchURI = Manager.GetApplicationConfigValue("ElasticSearch", "AutomationServices.EventAggregation.Providers.Cassandra");

        public static bool CreateIndex(string index, string type, string key, string payload)
        {
            if (string.IsNullOrEmpty(index) || string.IsNullOrEmpty(type) || string.IsNullOrEmpty(key) || string.IsNullOrEmpty(payload))
                throw new Exception("Need all parameters to create an index");

            // # encode the key 
            key = HttpUtility.UrlEncode(key);

            string uri = ElasticSearchURI + index + "/" + type + "/" + key;
            int statusCode = 0;
            string statusDescription = "";
            WebHeaderCollection headers = new WebHeaderCollection();
            headers.Add("Content-Type", "application/json");
            AddAuthHeader(headers);
            SetSecurityProtocol();

            DateTime startTime = DateTime.UtcNow;
            WebPageRequest.HTTPPost(uri, payload, ref statusCode, ref statusDescription, ref headers);
            RecordMetrics(index, POST, 200, startTime, DateTime.UtcNow);
            return true;
        }

        public static bool GetOnKey(string index, string type, string key)
        {
            if (string.IsNullOrEmpty(index) || string.IsNullOrEmpty(type) || string.IsNullOrEmpty(key))
                throw new Exception("Need all parameters to Get on Key");

            string uri = ElasticSearchURI + index + "/" + type + "/" + key;
            int statusCode = 0;
            string statusDescription = "";
            WebHeaderCollection headers = null;
            AddAuthHeader(headers);
            SetSecurityProtocol();
            DateTime startTime = DateTime.UtcNow;
            var response = WebPageRequest.HTTPGet(uri, ref statusCode, ref statusDescription, ref headers);
            RecordMetrics(index, GET, 200, startTime, DateTime.UtcNow);
            return true;
        }

        public static string GetClientURL()
        {
            return ElasticSearchURI;
        }

        #region Nest

        /// <summary>
        /// Advanced search with Batch Size
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="expression"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public virtual List<T> ExecuteQueryWithBatchSize(T entity, string expression, int batchSize)
        {
            DateTime startTime = DateTime.UtcNow;
            if (expression.Trim().Equals("*"))
            {
                this.OpenConnection(ConnectionType.Search);
                var searchResult = this.ElasticClientProvider.Search<T>(s => s
                    .From(0)
                    .Size(batchSize)
                    .Query(q => q.MatchAll())
                );
                return searchResult.Documents.ToList();
            }
            var expressions = expression.Split(new[] { "Order By" }, StringSplitOptions.None);
            var whereClause = expressions[0].Trim();
            var orderBy = expressions.Length > 1 ? expressions[1] : string.Empty;

            var validateInput = whereClause.Replace("begins with", "like").Replace("ends with", "like");
            this.ValidateExpression(validateInput);

            var sortFields = new List<ISort>();
            if (!string.IsNullOrEmpty(orderBy))
            {
                sortFields = orderBy.Split(',').Select(x =>
                    new SortField
                    {
                        Field = x.Trim().Split(' ')[0],
                        Order = x.Trim().Split(' ')[1].Equals("desc", StringComparison.CurrentCultureIgnoreCase)
                                ? Nest.SortOrder.Descending : Nest.SortOrder.Ascending
                    }).ToList<ISort>();
            }

            var request = new SearchRequest
            {
                From = 0,
                Size = batchSize,
                Sort = sortFields,
                Query = this.CreateRequest(whereClause),

            };

            if (request.Query == null)
                return default(List<T>);

            this.OpenConnection(ConnectionType.Search);
            var response = this.ElasticClientProvider.Search<T>(request);
            RecordMetrics(_indexName, GET, 200, startTime, DateTime.UtcNow);
            return response.Documents.ToList();
        }

        /// <summary>
        /// Execute the query with batch size
        /// </summary>
        /// <param name="entity">entity</param>
        /// <param name="expression">expression</param>
        /// <param name="batchSize">No of records to be return</param>
        /// <param name="indexName">name of the index</param>
        /// <param name="typeName">index type</param>
        /// <returns>list of entity</returns>
        public virtual List<T> ExecuteQueryWithBatchSize(T entity, string expression, int batchSize, string indexName, string typeName)
        {
            this._indexName = indexName;
            this._typeName = typeName;
            return this.ExecuteQueryWithBatchSize(entity, expression, batchSize);
        }

        /// <summary>
        /// Advanced search
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        public virtual List<T> ExecuteQuery(T entity, string expression)
        {
            return this.ExecuteQueryWithBatchSize(entity, expression, DefaultBatchSize);
        }

        /// <summary>
        /// execute the query for the given index
        /// </summary>
        /// <param name="entity">name of the entity</param>
        /// <param name="expression">query</param>
        /// <param name="indexName">name of the index</param>
        /// <param name="typeName">index type</param>
        /// <returns>list of entity</returns>
        public virtual List<T> ExecuteQuery(T entity, string expression, string indexName, string typeName)
        {
            this._indexName = indexName;
            this._typeName = typeName;
            return this.ExecuteQueryWithBatchSize(entity, expression, DefaultBatchSize);
        }

        /// <summary>
        /// Advanced search
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="expression"></param>
        /// <param name="pagination"></param>
        /// <returns></returns>
        public virtual Pagination<T> GetPagedObjects(T entity, string expression, Pagination<T> pagination)
        {
            DateTime startTime = DateTime.UtcNow;
            if (expression.Trim().Equals("*"))
            {
                OpenConnection(ConnectionType.Search);
                var sortDescriptor = new SortDescriptor<T>();
                if (!string.IsNullOrEmpty(pagination.SortField))
                {
                    string[] sortFields = pagination.SortField.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
                    foreach (var sortField in sortFields)
                        sortDescriptor.Field(sortField.Trim(), pagination.Sort == Sort.asc ? Nest.SortOrder.Ascending : Nest.SortOrder.Descending);
                }
                var searchResult = ElasticClientProvider.Search<T>(s => s
                        .From(pagination.From)
                        .Size(pagination.PageSize)
                        .Query(q => q.MatchAll())
                        .Sort(sort => sortDescriptor)
                    );

                pagination.ResultSet = new Result<T>() { TotalRecords = searchResult.Total, ResultSet = searchResult.Documents.ToList() };                
            }
            else
            {
                var validateInput = expression.Replace("begins with", "like").Replace("ends with", "like");
                ValidateExpression(expression);

                var request = new SearchRequest
                {
                    From = pagination.From,
                    Size = pagination.PageSize,
                    Sort = pagination.SortField.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries).Select(x =>
                        new SortField
                        {
                            Field = x.Trim(),
                            Order = pagination.Sort == Sort.asc ? Nest.SortOrder.Ascending : Nest.SortOrder.Descending
                        }).ToList<ISort>(),
                    Query = CreateRequest(expression.Trim())
                };

                if (request.Query == null)
                    return default(Pagination<T>);

                OpenConnection(ConnectionType.Search);
                var response = ElasticClientProvider.Search<T>(request);
                
                pagination.ResultSet = new Result<T>() { TotalRecords = response.Total, ResultSet = response.Documents.ToList() };
            }

            RecordMetrics(_indexName, GET, 200, startTime, DateTime.UtcNow);
            return pagination;
        }

        /// <summary>
        /// Create request object
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        private QueryContainer CreateRequest(string expression)
        {
            var matches = Regex.Matches(expression, "BETWEEN(.*?)AND", RegexOptions.IgnoreCase);
            foreach (Match match in matches)
            {
                expression = Regex.Replace(
                    expression, string.Format(@"\b{0}\b", match.Value),
                    string.Format("BETWEEN {0} 0x7f ", match.Groups[1].Value.Trim()));
            }

            expression = expression.StartsWith("(") && expression.EndsWith(")") ? expression.Substring(1, expression.Length - 2) : expression;

            matches = Regex.Matches(expression, @"\((.*?)\)", RegexOptions.IgnoreCase);
            int expressionIndex = 0;
            Dictionary<string, QueryContainer> expressionBuilder = new Dictionary<string, QueryContainer>();

            foreach (Match match in matches)
            {
                var innerExpression = match.Value.Trim();
                string key = string.Format("expression_{0}", expressionIndex++);
                expression = expression.Replace(innerExpression, key);
                expressionBuilder.Add(key, BuildInnerExpression(innerExpression));
            }

            QueryContainer finalQuery = null;
            expression = expression.Replace("''", ConditionWithEmptySpace);
            var operation_matches = Regex.Matches(expression, logicaloperator_pattern, RegexOptions.IgnoreCase);
            LogicalOperator operators = LogicalOperator.AND;
            foreach (Match operation_match in operation_matches)
            {
                var outerExpression = operation_match.Value.Trim();
                if (Enum.IsDefined(typeof(LogicalOperator), operators))
                {
                    if (expressionBuilder.ContainsKey(outerExpression))
                    {
                        if (operators == LogicalOperator.AND)
                            finalQuery &= expressionBuilder[outerExpression];
                        else
                            finalQuery |= expressionBuilder[outerExpression];
                    }
                    else
                        AssignQueryContainer(outerExpression, operators, ref finalQuery);
                }
                else
                    throw new ArgumentException("Inavlid logical operators");

                if (!string.IsNullOrEmpty(operation_match.NextMatch().Groups[1].Value))
                    operators = (LogicalOperator)Enum.Parse(typeof(LogicalOperator), operation_match.NextMatch().Groups[1].Value, true);
            }

            return finalQuery;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="innerExpression"></param>
        /// <returns></returns>
        private QueryContainer BuildInnerExpression(string innerExpression)
        {
            QueryContainer query = null;

            innerExpression = innerExpression.StartsWith("(") && innerExpression.EndsWith(")") ? innerExpression.Substring(1, innerExpression.Length - 2) : innerExpression;

            innerExpression = innerExpression.Replace("''", ConditionWithEmptySpace);
            var innerOperationMatches = Regex.Matches(innerExpression, logicaloperator_pattern, RegexOptions.IgnoreCase);
            LogicalOperator operators = LogicalOperator.AND;
            foreach (Match operation_match in innerOperationMatches)
            {
                if (Enum.IsDefined(typeof(LogicalOperator), operators))
                    AssignQueryContainer(operation_match.Value.Trim(), operators, ref query);
                else
                    throw new ArgumentException("Inavlid logical operators");

                if (!string.IsNullOrEmpty(operation_match.NextMatch().Groups[1].Value))
                    operators = (LogicalOperator)Enum.Parse(typeof(LogicalOperator), operation_match.NextMatch().Groups[1].Value, true);
            }

            return query;
        }

        /// <summary>
        /// Validate expression
        /// </summary>
        /// <param name="expression"></param>
        private void ValidateExpression(string expression)
        {
            bool fQuotedIdenfifiers = false;
            TSql100Parser _parser = new TSql100Parser(fQuotedIdenfifiers);

            SqlScriptGeneratorOptions options = new SqlScriptGeneratorOptions();
            options.SqlVersion = SqlVersion.Sql100;
            options.KeywordCasing = KeywordCasing.Uppercase;
            Sql100ScriptGenerator _scriptGen = new Sql100ScriptGenerator(options);

            IList<ParseError> errors;
            string inputScript = string.Format("SELECT 1 FROM Template WHERE {0}", expression);
            using (System.IO.StringReader sr = new System.IO.StringReader(inputScript))
            {
                IScriptFragment fragment = _parser.Parse(sr, out errors);
            }

            if (errors != null && errors.Count > 0) 
                throw new Exception(string.Join(",", errors.Select(x => x.Message).ToList<string>()));
        }

        /// <summary>
        /// Create query container
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="operators"></param>
        /// <param name="query"></param>
        private void AssignQueryContainer(string expression, LogicalOperator operators, ref QueryContainer query)
        {
            var matches = Regex.Split(expression, condition_pattern, RegexOptions.IgnoreCase);
            if (matches.Length < 3)
                throw new ArgumentException(string.Format("The string contains an inavlid condition : {0}", expression));

            var field = matches[0].Trim();
            var condition = matches[1].Trim().ToLower();
            var fieldValue = matches[2].Trim().Trim('\'');

            var valueSplit = fieldValue.Split(new string[] { "0x7f" }, StringSplitOptions.None);
            var value = valueSplit[0].ToString().Trim();
            var value_2 = valueSplit.Length > 1 ? valueSplit[1].ToString().Trim() : string.Empty;

            switch (condition)
            {
                case "=":
                    if (operators == LogicalOperator.AND)
                        query &= new TermQuery { Field = field, Value = value };
                    else
                        query |= new TermQuery { Field = field, Value = value };
                    break;
                case "!=":
                    if (operators == LogicalOperator.AND)
                        query &= !new TermQuery { Field = field, Value = value };
                    else
                        query |= !new TermQuery { Field = field, Value = value };
                    break;
                case ">=":
                    if (operators == LogicalOperator.AND)
                        query &= new TermRangeQuery { Field = field, GreaterThanOrEqualTo = value };
                    else
                        query |= new TermRangeQuery { Field = field, GreaterThanOrEqualTo = value };
                    break;
                case "<=":
                    if (operators == LogicalOperator.AND)
                        query &= new TermRangeQuery { Field = field, LessThanOrEqualTo = value };
                    else
                        query |= new TermRangeQuery { Field = field, LessThanOrEqualTo = value };
                    break;
                case ">":
                    if (operators == LogicalOperator.AND)
                        query &= new TermRangeQuery { Field = field, GreaterThan = value };
                    else
                        query |= new TermRangeQuery { Field = field, GreaterThan = value };
                    break;
                case "<":
                    if (operators == LogicalOperator.AND)
                        query &= new TermRangeQuery { Field = field, LessThan = value };
                    else
                        query |= new TermRangeQuery { Field = field, LessThan = value };
                    break;
                case "contains":
                    value = value.Trim().TrimStart('(').TrimEnd(')');
                    var termsContains = Regex.Matches(value, @"'[^']*'").Cast<Match>().Select(x => x.Value.Trim().Trim('\'')).ToList<object>();
                    if (termsContains == null || termsContains.Count <= 0)
                        termsContains = value.Split(new string[] { "," }, StringSplitOptions.None).ToList<object>();

                    if (operators == LogicalOperator.AND)
                        query &= new TermsQuery { Field = field, Terms = termsContains };
                    else
                        query |= new TermsQuery { Field = field, Terms = termsContains };
                    break;
                case "not contains":
                    value = value.Trim().TrimStart('(').TrimEnd(')');
                    var termsNotContains = Regex.Matches(value, @"'[^']*'").Cast<Match>().Select(x => x.Value.Trim().Trim('\'')).ToList<object>();
                    if (termsNotContains == null || termsNotContains.Count <= 0)
                        termsNotContains = value.Split(new string[] { "," }, StringSplitOptions.None).ToList<object>();

                    if (operators == LogicalOperator.AND)
                        query &= !new TermsQuery { Field = field, Terms = termsNotContains };
                    else
                        query |= !new TermsQuery { Field = field, Terms = termsNotContains };
                    break;
                case "like":
                    if (operators == LogicalOperator.AND)
                        query &= new WildcardQuery { Field = field, Value = value };
                    else
                        query |= new WildcardQuery { Field = field, Value = value };
                    break;
                case "begins with":
                    if (operators == LogicalOperator.AND)
                        query &= new PrefixQuery { Field = field, Value = value };
                    else
                        query |= new PrefixQuery { Field = field, Value = value };
                    break;
                case "ends with":
                    if (operators == LogicalOperator.AND)
                        query &= new WildcardQuery { Field = field, Value = string.Format("*{0}", value) };
                    else
                        query |= new WildcardQuery { Field = field, Value = string.Format("*{0}", value) };
                    break;
                case "between":
                    if (operators == LogicalOperator.AND)
                        query &= new TermRangeQuery { Field = field, GreaterThanOrEqualTo = value, LessThanOrEqualTo = value_2 };
                    else
                        query |= new TermRangeQuery { Field = field, GreaterThanOrEqualTo = value, LessThanOrEqualTo = value_2 };
                    break;
                default:
                    throw new ArgumentException(string.Format("Invalid condition for : {0}", expression));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="uniqueKey"></param>
        public virtual void CreateIndex(T entity, string uniqueKey)
        {
            DateTime startTime = DateTime.UtcNow;
            PostDatatoElasticSearchServer(entity, uniqueKey);
            this.RecordMetricsGraph(POST, "CreateIndex", startTime, DateTime.UtcNow);
        }

        /// <summary>
        /// Create a record for the key
        /// </summary>
        /// <param name="entity">entity</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="indexName">Index Name</param>
        /// <param name="typeName">Type Name</param>
        /// <returns>true or false</returns>
        public virtual bool CreateIndex(T entity, string uniqueKey, string indexName, string typeName)
        {
            DateTime startTime = DateTime.UtcNow;
            this._indexName = indexName;
            this._typeName = typeName;
            bool status = this.PostDatatoElasticSearchServer(entity, uniqueKey);
            this.RecordMetricsGraph(POST, "CreateIndex", startTime, DateTime.UtcNow);
            return status;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="uniqueKey"></param>
        public virtual void UpsertIndex(T entity, string uniqueKey)
        {
            PostDatatoElasticSearchServer(entity, uniqueKey);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="payload"></param>
        /// <param name="uniqueKey"></param>
        public virtual bool UpsertIndex(string payload, string uniqueKey)
        {
            return PostDatatoElasticSearchServer(payload, uniqueKey);
        }

        /// <summary>
        /// Create a record for the key
        /// </summary>
        /// <param name="entity">entity</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="indexName">Index Name</param>
        /// <param name="typeName">Type Name</param>
        /// <returns>true or false</returns>
        public virtual bool UpsertIndex(T entity, string uniqueKey, string indexName, string typeName)
        {
            this._indexName = indexName;
            this._typeName = typeName;
            return this.PostDatatoElasticSearchServer(entity, uniqueKey);
        }

        /// <summary>
        /// Create a record for the key
        /// </summary>
        /// <param name="payload">payload</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="indexName">Index Name</param>
        /// <param name="typeName">Type Name</param>
        /// <returns>true or false</returns>
        public virtual bool UpsertIndex(string payload, string uniqueKey, string indexName, string typeName)
        {
            this._indexName = indexName;
            this._typeName = typeName;
            return this.PostDatatoElasticSearchServer(payload, uniqueKey);
        }

        private bool PostDatatoElasticSearchServer(string payload, string uniqueKey)
        {
            DateTime startTime = DateTime.UtcNow;
            try
            {
                int statusCode = 0;
                string statusDescription = string.Empty;
                string uri = string.Format("{0}{1}/{2}/{3}/_update", ElasticSearchURI, _indexName.ToLower(), _typeName, HttpUtility.UrlEncode(uniqueKey));
                WebHeaderCollection headers = new WebHeaderCollection();
                headers.Add("Content-Type", "application/json");
                AddAuthHeader(headers);
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
                string payload_internal = "{\"doc\": " + payload + ",\"doc_as_upsert\" : true }";
                this.PostDatatoElasticSearchServer(uri, "POST", ContentType, payload_internal, ref statusCode, ref statusDescription, ref headers);

                return statusCode == 200 || statusCode == 201 || statusCode == 202;
            }
            finally
            {
                this.RecordMetricsGraph(POST, "PostDatatoElasticSearchServer", startTime, DateTime.UtcNow);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="uniqueKey"></param>
        /// <returns></returns>
        private bool PostDatatoElasticSearchServer(T entity, string uniqueKey)
        {
            DateTime startTime = DateTime.UtcNow;
            try
            {
                int statusCode = 0;
                string statusDescription = string.Empty;
                string uri = string.Format("{0}{1}/{2}/{3}", ElasticSearchURI, _indexName.ToLower(), _typeName, HttpUtility.UrlEncode(uniqueKey));
                WebHeaderCollection headers = new WebHeaderCollection();
                headers.Add("Content-Type", "application/json");
                AddAuthHeader(headers);
                string payload = JSon.Serialize<T>(entity);
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
                this.PostDatatoElasticSearchServer(uri, "POST", ContentType, payload, ref statusCode, ref statusDescription, ref headers);

                return statusCode == 200 || statusCode == 201 || statusCode == 202;
            }
            finally
            {
                this.RecordMetricsGraph(POST, "PostDatatoElasticSearchServer 2", startTime, DateTime.UtcNow);
            }
        }

        private bool BulkUpdates(object[] requestEntity)
        {
            DateTime startTime = DateTime.UtcNow;
            try
            {
                int statusCode = 0;
                string statusDescription = string.Empty;
                string uri = string.Format("{0}{1}/{2}/_bulk", ElasticSearchURI, this._indexName.ToLower(), this._typeName);
                WebHeaderCollection headers = new WebHeaderCollection();
                headers.Add("Content-Type", "application/json");
                AddAuthHeader(headers);
                string payload = JSon.Serialize<object[]>(requestEntity);
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
                this.PostDatatoElasticSearchServer(uri, "put", ContentType, payload, ref statusCode, ref statusDescription, ref headers);

                return statusCode == 200 || statusCode == 201 || statusCode == 202;
            }
            finally
            {
                this.RecordMetricsGraph(BULK, "BulkUpdates", startTime, DateTime.UtcNow);
            }
        }

        private ElasticResponse<T> SearchByQuery(string query)
        {
            DateTime startTime = DateTime.UtcNow;
            try
            {
                int statusCode = 0;
                string statusDescription = string.Empty;
                string uri = string.Format("{0}{1}/{2}/_search", ElasticSearchURI, this._indexName.ToLower(), this._typeName);
                WebHeaderCollection headers = new WebHeaderCollection();
                headers.Add("Content-Type", "application/json");
                AddAuthHeader(headers);
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
                var response = this.PostDatatoElasticSearchServer(uri, "POST", ContentType, query, ref statusCode, ref statusDescription, ref headers);

                return statusCode == 200 ? JSon.Deserialize<ElasticResponse<T>>(response) : default(ElasticResponse<T>);
            }
            finally
            {
                this.RecordMetricsGraph(GET, "SearchByQuery", startTime, DateTime.UtcNow);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="uniqueKey"></param>
        public virtual void DeleteIndex(T entity, string uniqueKey)
        {
            DateTime startTime = DateTime.UtcNow;
            try
            {
                OpenConnection(ConnectionType.Delete);
                DeleteRequest<T> deleteRequest = new DeleteRequest<T>(_indexName.ToLower(), _typeName, uniqueKey)
                {
                    Refresh = true
                };
                ElasticClientProvider.Delete(deleteRequest);
            }
            finally
            {
                RecordMetrics(_indexName, DELETE, 200, startTime, DateTime.UtcNow);
            }
        }

        /// <summary>
        /// delete index for the given key
        /// </summary>
        /// <param name="entity">entity</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="indexName">Index Name</param>
        /// <param name="typeName">Type Name</param>
        public void DeleteIndex(T entity, string uniqueKey, string indexName, string typeName)
        {
            this._indexName = indexName;
            this._typeName = typeName;
            this.DeleteIndex(entity, uniqueKey);
        }

        public bool UpdateIndex(string payload, string uniqueKey, int requestTimeout)
        {
            DateTime startTime = DateTime.UtcNow;
            try
            {
                int statusCode = 0;
                string statusDescription = string.Empty;
                string uri = string.Format("{0}{1}/{2}/{3}/_update", ElasticSearchURI, this._indexName.ToLower(), this._typeName, HttpUtility.UrlEncode(uniqueKey));
                WebHeaderCollection headers = new WebHeaderCollection();
                headers.Add("Content-Type", "application/json");
                AddAuthHeader(headers);
                ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
                this.PostDatatoElasticSearchServer(uri, "POST", ContentType, payload, ref statusCode, ref statusDescription, ref headers, requestTimeout);

                return statusCode == 200 || statusCode == 201 || statusCode == 202;
            }
            finally
            {
                this.RecordMetricsGraph(POST, "UpdateIndex", startTime, DateTime.UtcNow);
            }
        }

        /// <summary>
        /// update index for the given key
        /// </summary>
        /// <param name="payload">payload</param>
        /// <param name="uniqueKey">Unique Key</param>
        /// <param name="requestTimeout">Request Timeout</param>
        /// <param name="indexName">Index Name</param>
        /// <param name="typeName">Type Name</param>
        /// <returns>true or false</returns>
        public bool UpdateIndex(string payload, string uniqueKey, int requestTimeout, string indexName, string typeName)
        {
            this._indexName = indexName;
            this._typeName = typeName;
            return this.UpdateIndex(payload, uniqueKey, requestTimeout);
        }

        private static void RecordMetrics(string indexName, string requestType, int statusCode, DateTime startTime, DateTime endTime)
        {
            try
            {
                var graphProvider = Container.Resolve<IGraphProvider>();
                if (graphProvider != null)
                {
                    string graphPath = string.Format("es_indices_mtx.{0}.{1}.{2}", requestType, GetIndexShortName(indexName).Replace('.', '_'), statusCode).ToLower();
                    graphProvider.RecordAt(new Point(
                        string.Format("{0}.lat", graphPath), OPSManager.Parser.ToInt((endTime - startTime).TotalMilliseconds, 0)));
                    graphProvider.RecordAt(new Point(
                        string.Format("{0}.cnt", graphPath)));
                }
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.API, ClassName, "Data layer: Failed to record metrics", new object[] { indexName, requestType, startTime, endTime }, ex);
            }
        }

        private static string GetIndexShortName(string indexName)
        {
            Dictionary<string, string> keyValuePairs = JSon.Deserialize<Dictionary<string, string>>(Manager.GetApplicationConfigValue("IndexName.Short", "ElasticSearch"));
            string shortName = keyValuePairs != null ?
                keyValuePairs.FirstOrDefault(x => x.Key.Equals(indexName, StringComparison.CurrentCultureIgnoreCase) || indexName.Contains(x.Key)).Value : indexName;
            return shortName ?? indexName;
        }

        private string PostDatatoElasticSearchServer(string url, string requestMethod, string contentType, string content, ref int statusCode, ref string statusDescription, ref WebHeaderCollection headers)
        {
            string esRequestTimeout = ConfigurationManager.AppSettings["ES_RequestTimeout"];
            DateTime startTime = DateTime.UtcNow;
            var response = string.IsNullOrEmpty(esRequestTimeout) ?
                WebPageRequest.HTTPRequest(url, requestMethod, contentType, content, string.Empty, string.Empty, string.Empty, ref statusCode, ref statusDescription, ref headers, false) :
                WebPageRequest.HTTPRequest(url, requestMethod, contentType, content, string.Empty, string.Empty, string.Empty, ref statusCode, ref statusDescription, ref headers, false, OPSManager.Parser.ToInt(esRequestTimeout, 5000));
            RecordMetrics(_indexName, requestMethod, statusCode, startTime, DateTime.UtcNow);
            this.TraceException(requestMethod, statusCode, string.Format("Status Description : {0}, Response : {1}", statusDescription, response));
            return response;
        }

        private string PostDatatoElasticSearchServer(string url, string requestMethod, string contentType, string content, ref int statusCode, ref string statusDescription, ref WebHeaderCollection headers, int requestTimeout)
        {
            DateTime startTime = DateTime.UtcNow;
            var response = WebPageRequest.HTTPRequest(url, requestMethod, contentType, content, string.Empty, string.Empty, string.Empty, ref statusCode, ref statusDescription, ref headers, false, requestTimeout);
            RecordMetrics(_indexName, requestMethod, statusCode, startTime, DateTime.UtcNow);
            this.TraceException(requestMethod, statusCode, string.Format("Status Description : {0}, Response : {1}", statusDescription, response));
            return response;
        }

        /// <summary>
        /// Add Auth Token to Request Header
        /// </summary>
        /// <param name="headerCollection">Header Collection</param>
        private static void AddAuthHeader(WebHeaderCollection headerCollection)
        {
            if (IsAuthEnabled)
            {
                if (headerCollection == null)
                {
                    headerCollection = new WebHeaderCollection();
                }

                if (!headerCollection.AllKeys.Any(x => x.Equals(AuthToken, StringComparison.CurrentCultureIgnoreCase)))
                {
                    string authToken = GetAuthToken;
                    if (string.IsNullOrEmpty(authToken))
                    {
                        throw new Exception("AuthToken is empty. Access denied");
                    }

                    headerCollection.Add(AuthToken, authToken);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="uniqueKey"></param>
        /// <returns></returns>
        private bool DeleteIndex(string uniqueKey)
        {
            DateTime startTime = DateTime.UtcNow;
            try
            {
                this.OpenConnection(ConnectionType.Delete);
                DeleteRequest<T> deleteRequest = new DeleteRequest<T>(this._indexName.ToLower(), this._typeName, uniqueKey)
                {
                    Refresh = true
                };
                return this.ElasticClientProvider.Delete(deleteRequest).Found;
            }
            finally
            {
                this.RecordMetricsGraph(DELETE, "DeleteIndex2", startTime, DateTime.UtcNow);
            }
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sortFields"></param>
        /// <param name="sortType"></param>
        /// <returns></returns>
        private List<Dictionary<string, string>> GetSortFields(string sortFields, string sortType)
        {
            List<Dictionary<string, string>> dicReturn = new List<Dictionary<string, string>>();
            string[] fields = sortFields.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var sortField in fields)
                dicReturn.Add(new Dictionary<string, string> { { sortField.Trim(), sortType } });

            return dicReturn;
        }

        private void RecordMetricsGraph(string requestType, string methodName, DateTime startTime, DateTime endTime)
        {
            try
            {
                var graphProvider = Container.Resolve<IGraphProvider>();
                if (graphProvider != null)
                {
                    graphProvider.RecordAt(new Point(
                        string.Format("elasticsearch.{0}.{1}.{2}.lat", this._indexName, requestType, methodName), OPSManager.Parser.ToInt((endTime - startTime).TotalMilliseconds, 0)));
                }
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.API, ClassName, "Data layer: Failed to record metrics", new object[] { this._indexName, requestType, methodName, startTime, endTime }, ex);
            }
        }

        private void TraceException(string requestType, int statusCode, string exception)
        {
            try
            {
                if (!OPSManager.Parser.ToBoolean(Manager.GetApplicationConfigValue("ElasticSearch.TraceException.Enabled", "FeatureFlag"), false))
                {
                    return;
                }

                Trace.Exception(TraceTypes.API, ClassName, "Data layer: Failed to record metrics", new object[] { this._indexName, requestType, statusCode }, new Exception(exception));
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.API, ClassName, "TraceException", new object[] { this._indexName, requestType, statusCode }, ex);
            }
        }
    }
}
