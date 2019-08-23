using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using automation.components.data.v1.Buffer;
using automation.components.data.v1.Graph;
using automation.components.data.v1.Graph.Providers;
using automation.components.data.v1.Config;
using automation.components.operations.v1;
using Dse;
using Trace = automation.components.operations.v1.Trace;
using Dse.Mapping;
using automation.components.data.v1.Entities;

namespace automation.components.data.v1.Cassandra
{
    /// <summary>
    /// DataStax Connection Provider
    /// </summary>
    /// <typeparam name="T">Dynamic object</typeparam>
    public class DSEDataExplorer : ICassandraDataExplorer, IDisposable
    {
        /// <summary>
        /// Gets or sets gets or Sets Key Column Name
        /// </summary>
        protected const string KeyColumnName = "key";

        /// <summary>
        /// Gets or sets gets or Sets JSon Column Name
        /// </summary>
        protected const string JSonColumnName = "value";

        /// <summary>
        /// Gets or sets gets or Sets Default Limit
        /// </summary>
        protected const int DefaultLimit = 1000;

        private IGraphProvider graphProvider;

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

        private bool UsePreparedStatement
        {
            get
            {
                return Parser.ToBoolean(Manager.GetApplicationConfigValue("RBAN-9158", "FeatureFlag"), false);
            }
        }

        private bool UsePreparedStatementGetCalls
        {
            get
            {
                return Parser.ToBoolean(Manager.GetApplicationConfigValue("RBAN-9426", "FeatureFlag"), false);
            }
        }

        private bool RemoveDuplicateData
        {
            get
            {
                return Parser.ToBoolean(Manager.GetApplicationConfigValue("ARIC-10953", "FeatureFlag"), false);
            }
        }

        /// <summary>
        /// Prepared Statement Cache
        /// </summary>
        private static ConcurrentDictionary<string, PreparedStatement> preparedStatementCache = new ConcurrentDictionary<string, PreparedStatement>();

        /// <summary>
        /// Gets or sets gets or Sets Session
        /// </summary>
        protected ISession Session;

        public DSEDataExplorer(string keyspace, string columnFamily, string keyPrefix = "")
        {
            this.KeySpace = keyspace;
            this.ColumnFamily = columnFamily;
            this.KeyPrefix = keyPrefix;

            this.graphProvider = Container.Resolve<IGraphProvider>();
        }

        public virtual List<T> GetIndexes<T>(string columnFamily, Dictionary<string, string> whereColumnValues)
        {
            return this.GetIndexes<T>(columnFamily, new List<string>(), whereColumnValues, 0);
        }

        public virtual List<T> GetIndexes<T>(string columnFamily, Dictionary<string, string> whereColumnValues, int limit)
        {
            return this.GetIndexes<T>(columnFamily, new List<string>(), whereColumnValues, limit);
        }

        public virtual List<T> GetIndexes<T>(string columnFamily, List<string> returnColumn, Dictionary<string, string> whereColumnValues)
        {
            return this.GetIndexes<T>(columnFamily, returnColumn, whereColumnValues, 0);
        }

        public virtual List<T> GetIndexes<T>(string columnFamily, List<string> returnColumn, Dictionary<string, string> whereColumnValues, int limit)
        {
            if (whereColumnValues == null || !whereColumnValues.Any())
                return default(List<T>);
            var st = DateTime.UtcNow;

            List<T> results;
            if (this.UsePreparedStatement)
            {
                Dictionary<string, object> whereClause = new Dictionary<string, object>();
                foreach (var kvp in whereColumnValues)
                {
                    whereClause[kvp.Key] = kvp.Value;
                }
               var data = this.JoinedAsyncQuery(columnFamily, returnColumn,
                    new Dictionary<int, Dictionary<string, object>> { { 1, whereClause } }, limit, true);
                results = data != null && data.Count > 0
                    ? data.Where(x => x != null && x.GetColumn("[json]") != null)
                        .Select(x => JSon.Deserialize<T>(x.GetValue<string>("[json]")))
                        .ToList()
                    : new List<T>();
            }
            else
            {
                string cqlQuery = string.Format("select {2} from {0}.{1} where {3} {4};", this.KeySpace, columnFamily,
                    returnColumn == null || !returnColumn.Any() ? "*" : string.Join(", ", returnColumn),
                    string.Join(" and ", whereColumnValues.Select(x => string.Format("{0} = '{1}'", x.Key, x.Value))),
                    this.GetLimitQuery(limit));

                results = this.ExecuteQuery<T>(cqlQuery);
            }

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndexes, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return results;
        }

        public virtual List<T> GetIndexesAsObject<T>(string columnFamily, List<string> returnColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne, bool executeWithCLLocalQuorum)
        {
            if (whereClause == null || !whereClause.Any())
                return default(List<T>);

            var st = DateTime.UtcNow;

            var results = this.JoinedAsyncQuery(columnFamily, returnColumn, whereClause, limit, true, executeWithCLOne,
                executeWithCLLocalQuorum, false);

            var response = results != null && results.Count > 0 ?
                results.Where(x => x != null && x.GetColumn("[json]") != null).Select(x => JSon.Deserialize<T>(x.GetValue<string>("[json]"))).ToList() : default(List<T>);

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndexesAsObject, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return response;
        }

        public virtual bool DeleteIndex(string columnFamily, Dictionary<string, object> whereColumnValues)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);

            string cqlQuery = string.Format("delete from {0}.{1} where {2} = ?;", this.KeySpace, columnFamily, string.Join(" = ? AND ", whereColumnValues.Keys));

            var statement = this.CachePrepare(cqlQuery);
            BoundStatement bind = statement.Bind(whereColumnValues.Select(x => x.Value).ToArray());

            this.ExecuteNonQuery(bind);

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_DeleteIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return true;
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
            try
            {
                var tasks = new List<Task>();
                var st = DateTime.UtcNow;

                columnFamily = this.ValidateName("columnFamily", columnFamily);

                var session = this.GetCassandraSession();
                CancellationTokenSource cts = new CancellationTokenSource();

                string cqlQuery = string.Format("update {0}.{1} set {2} = ? {4}{5} where {3} = ?", this.KeySpace,
                    columnFamily, string.Join(" = ?, ", setColumnValues.Keys),
                    string.Join(" = ? AND ", whereColumnValues.Keys),
                    (autoTimeUUIDValues != null && autoTimeUUIDValues.ContainsKey("EnableAutoTimeUUID")
                    && autoTimeUUIDValues["EnableAutoTimeUUID"])
                    ? ", modification_timestamp"
                    : string.Empty,
                    (autoTimeUUIDValues != null && autoTimeUUIDValues.ContainsKey("EnableAutoTimeUUID") 
                    && autoTimeUUIDValues["EnableAutoTimeUUID"]) 
                    ? "= now()" 
                    : string.Empty);

                var parameters = new List<object>();
                parameters.AddRange(BuildBindVars(columnFamily, setColumnValues.Select(x => x.Value).ToArray()));
                parameters.AddRange(whereColumnValues.Select(x => x.Value).ToArray());

                if (!string.IsNullOrEmpty(cqlQuery))
                {
                    var statement = this.CachePrepare(cqlQuery);
                    BoundStatement bind = statement.Bind(parameters.ToArray());
                    var results = session.ExecuteAsync(bind);
                    tasks.Add(results);
                }

                if (tasks.Any())
                    WaitUnlessFault(tasks.ToArray(), cts.Token);

                //Record TimeTaken
                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Update, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "Update",
                    new object[] { columnFamily, whereColumnValues, setColumnValues },
                    ex);
                
                this.RecordConnectionFailed(ex.Message);
                throw;
            }
            return true;
        }

        public virtual bool UpsertCounter(string columnFamily, List<CounterIndex> indexes)
        {
            try
            {
                var st = DateTime.UtcNow;

                columnFamily = this.ValidateName("columnFamily", columnFamily);

                var session = this.GetCassandraSession();
                CancellationTokenSource cts = new CancellationTokenSource();

                var tasks = new List<Task>();
                foreach (var row in indexes)
                {
                    string cqlQuery = string.Empty;
                    switch (row.Type)
                    {
                        case CounterType.Increment:
                            cqlQuery = string.Format("update {0}.{1} set {2} = {2} + {3} where {4} = ?", this.KeySpace,
                                columnFamily, row.Name, row.Value, string.Join(" = ? AND ", row.KeyValues.Keys));
                            break;
                        case CounterType.Decrement:
                            cqlQuery = string.Format("update {0}.{1} set {2} = {2} - {3} where {4} = ?", this.KeySpace,
                                columnFamily, row.Name, row.Value, string.Join(" = ? AND ", row.KeyValues.Keys));
                            break;
                    }

                    if (!string.IsNullOrEmpty(cqlQuery))
                    {
                        var statement = this.CachePrepare(cqlQuery);
                        BoundStatement bind = statement.Bind(row.KeyValues.Select(x => x.Value).ToArray());
                        var resultSetFuture = session.ExecuteAsync(bind);
                        tasks.Add(resultSetFuture);
                    }
                }

                if (tasks.Any())
                    WaitUnlessFault(tasks.ToArray(), cts.Token);

                //Record TimeTaken
                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_UpsertCounter, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "UpsertCounter",
                    new object[] { columnFamily, indexes },
                    ex);

                //Ignore the trigger excpetion : this exception is thrown by cassandra if index already exists
                if (ex.Message.Contains("java.lang.RuntimeException: Exception while creating trigger on CF with ID"))
                {
                    return true;
                }

                this.RecordConnectionFailed(ex.Message);
                throw;
            }
            return true;
        }

        public virtual Dictionary<string, object> GetDataAsObject(string columnFamily, List<string> keys, string valueColumn)
        {
            if (keys == null || !keys.Any())
                return new Dictionary<string, object>();

            var st = DateTime.UtcNow;

            var results =
                this.JoinedAsyncQuery(
                    string.Format("select key, {2} from {0}.{1} where key=? limit {3};", this.KeySpace, columnFamily,
                        valueColumn, keys.Count), keys);

            Dictionary<string, object> colVals = this.TransformRows(results, "key", valueColumn);

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetDataAsObject, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual Dictionary<string, object> GetDataAsObject(string columnFamily, List<string> keys, string valueColumn, string keyColumn)
        {
            if (keys == null || !keys.Any())
                return new Dictionary<string, object>();

            var st = DateTime.UtcNow;

            var results =
                this.JoinedAsyncQuery(
                    string.Format("select {2}, {3} from {0}.{1} where {2}=? limit {4};", this.KeySpace, columnFamily,
                        keyColumn, valueColumn, keys.Count), keys);

            Dictionary<string, object> colVals = this.TransformRows(results, keyColumn, valueColumn);

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetDataAsObject, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual bool Add<T>(string key, T value)
        {
            return this.Add<T>(new List<DataExplorer<T>.Data> { new DataExplorer<T>.Data { Key = key, Value = value } }, 0);
        }

        public virtual bool Add<T>(string key, T value, uint timeToLive)
        {
            return this.Add<T>(new List<DataExplorer<T>.Data> { new DataExplorer<T>.Data { Key = key, Value = value } }, timeToLive);
        }

        public virtual bool Add<T>(DataExplorer<T>.Data data)
        {
            return this.Add<T>(new List<DataExplorer<T>.Data> { data }, 0);
        }

        public virtual bool Add<T>(DataExplorer<T>.Data data, uint timeToLive)
        {
            return this.Add<T>(new List<DataExplorer<T>.Data> { data }, timeToLive);
        }

        public virtual bool Add<T>(List<DataExplorer<T>.Data> data)
        {
            return this.Add<T>(data, 0);
        }

        public virtual bool ConnectivityCheck()
        {
            return this.GetCassandraSession() != null;
        }

        public virtual bool Add<T>(List<DataExplorer<T>.Data> data, uint timeToLive)
        {
            try
            {
                var st = DateTime.UtcNow;
                CancellationTokenSource cts = new CancellationTokenSource();

                var session = this.GetCassandraSession();
                var statementString = timeToLive == 0 
                    ? "insert into {0}.{1} (key, value) values (?, ?)" 
                    : "insert into {0}.{1} (key, value) values (?, ?) USING TTL ?";
                var formattedString = string.Format(statementString, this.KeySpace, this.ColumnFamily);
                var statement = this.CachePrepare(formattedString);

                WaitUnlessFault(
                    data.Select(
                        curData =>
                            timeToLive == 0
                                ? statement.Bind(BuildBindVars(this.ColumnFamily, curData.Key,
                                    JSon.Serialize(curData.Value)))
                                : statement.Bind(BuildBindVars(this.ColumnFamily, curData.Key,
                                    JSon.Serialize(curData.Value), (int) timeToLive)))
                        .Select(bind => session.ExecuteAsync(bind))
                        .Cast<Task>()
                        .ToArray(), cts.Token);

                var now = DateTime.UtcNow;
                //Record TimeTaken
                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Add, (int)(now - st).TotalMilliseconds));

                return true;
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "Add",
                    new object[] { data, timeToLive },
                    ex);

                if (ex.Message.Contains("Unable to read data from the transport connection:") ||
                    ex.Message.Contains("No connection could be made because all servers have failed.") ||
                    ex.Message.Contains("Connection to Cassandra has timed out") || ex.Message.Contains("The task didn't complete before timeout"))
                {
                    //Record TimeTaken
                    if (this.graphProvider != null)
                        this.graphProvider.Record(new Point(Constants.CassandraConnectionFailed));
                }

                throw;
            }
        }

        public virtual T Get<T>(string key)
        {
            return this.Get<T>(new List<string> { key }).FirstOrDefault();
        }

        /// <summary>
        /// This method is more expensive and execute a query in single node
        /// </summary>
        /// <param name="key"></param>
        /// <param name="executeInSingleNode"></param>
        /// <returns></returns>
        public virtual T Get<T>(string key, bool executeInSingleNode)
        {
            return this.Get<T>(new List<string> { key }, executeInSingleNode).FirstOrDefault();
        }

        public virtual List<T> Get<T>(int limit)
        {
            var results = this.ExecuteQuery(string.Format("select value from {0}.{1} {2};", this.KeySpace, this.ColumnFamily, this.GetLimitQuery(limit)));

            return this.TransformRowsAndDeserialize<T>(results, "value");
        }

        public virtual List<string> GetKeys()
        {
            var st = DateTime.UtcNow;
            var results =
                this.ExecuteQuery(string.Format("select key from {0}.{1} limit {2};", this.KeySpace,
                    this.ColumnFamily, int.MaxValue));

            List<string> colVals = this.TransformRows(results, "key");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Get,
                    (int) (DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<T> Get<T>(List<string> keys)
        {
            return this.GetData<T>(keys, false);
        }

        public virtual List<T> Get<T>(List<string> keys, bool executeInSingleNode)
        {
            return this.GetData<T>(keys, executeInSingleNode);
        }

        /// <summary>
        /// This method is more expensive and execute a query in single node
        /// </summary>
        /// <param name="keys">query keys</param>
        /// <param name="executeInSingleNode">execute on single not or not</param>
        /// <returns>list of columna vlaues</returns>
        private List<T> GetData<T>(List<string> keys, bool executeInSingleNode)
        {
            if (keys == null || !keys.Any())
            {
                return new List<T>();
            }
            var st = DateTime.UtcNow;
            var results = this.JoinedAsyncQuery(
                    string.Format(
                        "select value from {0}.{1} where key=?;", this.KeySpace, this.ColumnFamily), keys, executeInSingleNode);

            List<T> colVals = this.TransformRowsAndDeserialize<T>(results, "value");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Get, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual string GetAsString(string key)
        {
            return this.GetDataAsString(key, false);
        }

        /// <summary>
        /// This method is more expensive and execute a query in single node
        /// </summary>
        /// <param name="key"></param>
        /// <param name="executeInSingleNode"></param>
        /// <returns></returns>
        public virtual string GetAsString(string key, bool executeInSingleNode)
        {
            return this.GetDataAsString(key, executeInSingleNode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnFamily"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        public virtual Dictionary<string, string> GetAsString(string columnFamily, int limit)
        {
            return this.GetDataAsString(columnFamily, limit, false);
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
            return this.GetDataAsString(columnFamily, limit, executeInSingleNode);
        }

        private string GetDataAsString(string key, bool executeInSingleNode)
        {
            var st = DateTime.UtcNow;
            key = this.ValidateName("key", string.Format("{0}", key));
            IEnumerable<Row> results;
            if (this.UsePreparedStatementGetCalls)
            {
                results =
                    this.JoinedAsyncQuery(
                        string.Format("select value from {0}.{1} where key=? ;", this.KeySpace, this.ColumnFamily
                            ), new List<string>() {key}, executeInSingleNode);
            }
            else
            {
                results =
                    this.ExecuteQuery(
                        string.Format("select value from {0}.{1} where key  =  '{2}';", this.KeySpace,
                            this.ColumnFamily, key), executeInSingleNode);
            }
            
            var enumerable = results as Row[] ?? results.ToArray();
            var orDefault = enumerable.FirstOrDefault();
            string returnValue = orDefault != null && orDefault.Length > 0 ? orDefault.GetValue<string>("value") : string.Empty;
            
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Get, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return returnValue;
        }

        public virtual Dictionary<string, string> GetAsString(List<string> keys)
        {
            return this.GetDataAsString(this.ColumnFamily, keys, false);
        }

        public virtual T Get<T>(string columnFamily, string key)
        {
            return this.Get<T>(columnFamily, new List<string> { key }).FirstOrDefault();
        }

        /// <summary>
        /// This method is more expensive and execute a query in single node
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="executeInSingleNode"></param>
        /// <returns></returns>
        public virtual Dictionary<string, string> GetAsString(List<string> keys, bool executeInSingleNode)
        {
            return this.GetDataAsString(this.ColumnFamily, keys, executeInSingleNode);
        }

        public virtual Dictionary<string, string> GetAsString(string columnFamily, List<string> keys)
        {
            return this.GetDataAsString(columnFamily, keys, false);
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
            return this.GetDataAsString(columnFamily, keys, executeInSingleNode);
        }        

        public virtual List<T> Get<T>(string columnFamily, List<string> keys)
        {
            if (keys == null || !keys.Any())
                return new List<T>();

            var st = DateTime.UtcNow;
            var results =
                this.JoinedAsyncQuery(
                    string.Format("select value from {0}.{1} where key=? ;", this.KeySpace, columnFamily), keys);

            List<T> colVals = this.TransformRowsAndDeserialize<T>(results, "value");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Get, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        private Dictionary<string, string> GetDataAsString(string columnFamily, List<string> keys, bool executeInSingleNode)
        {
            if (keys == null || !keys.Any())
                return new Dictionary<string, string>();

            var st = DateTime.UtcNow;

            var results =
                this.JoinedAsyncQuery(
                    string.Format("select key, value from {0}.{1} where key=? limit {2};", this.KeySpace, columnFamily,
                        (keys.Count)), keys, executeInSingleNode);

            Dictionary<string, string> colVals = this.TransformRows<string>(results, "key", "value");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetAsString, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        private Dictionary<string, string> GetDataAsString(string columnFamily, int limit, bool executeInSingleNode)
        {
            var st = DateTime.UtcNow;

            var results =
                this.ExecuteQuery(
                    string.Format("select key, value from {0}.{1} {2};", this.KeySpace, columnFamily,
                        this.GetLimitQuery(limit)), executeInSingleNode);

            Dictionary<string, string> colVals = this.TransformRows<string>(results, "key", "value");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetDataAsString, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<string> Filter(string columnName, DateTime to)
        {
            return this.Filter(columnName, DateTime.MinValue, to, -1);
        }

        public virtual List<string> Filter(string columnName, DateTime to, int limit)
        {
            return this.Filter(columnName, DateTime.MinValue, to, limit);
        }

        public virtual List<string> Filter(string columnName, DateTime from, DateTime to)
        {
            return this.Filter(columnName, from, to, -1);
        }

        public virtual List<string> Filter(string columnName, DateTime from, DateTime to, int limit)
        {
            var st = DateTime.UtcNow;

            var results =
                this.ExecuteQuery(string.Format("select key from {0}.{1}{2} {3} ALLOW FILTERING;", this.KeySpace,
                    this.ColumnFamily, this.GetFromAndToQuery(columnName, " where ", from, to),
                    this.GetLimitQuery(limit)));

            List<string> strKeys =
                results.Where(x => x != null && x.GetColumn("key") != null && !x.IsNull("key"))
                    .Select(row => row.GetValue<string>("key"))
                    .Distinct()
                    .ToList();

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Filter, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return strKeys;
        }

        public virtual List<T> GetAllRows<T>(string columnFamily, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            var results = this.ExecuteQuery(string.Format("select * from {0}.{1} {2};", this.KeySpace, columnFamily, this.GetLimitQuery(limit)));

            var objectList = this.TransformRowsAndDeserialize<T>(results, "value");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetAllRows, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return objectList;
        }

        public virtual List<T> GetAllRows<T>(string columnFamily, List<string> requestColumn, int limit)
        {
            if (!Parser.ToString(Manager.GetApplicationConfigValue("GetAllRows.Restriction", "FeatureFlag"), string.Empty)
                .Split(',').Contains(columnFamily))
            {
                throw new Exception(string.Format("Column family: {0} is not allowed to use the functionality", columnFamily));
            }

            var st = DateTime.UtcNow;
            if (requestColumn == null || !requestColumn.Any())
                return default(List<T>);

            columnFamily = this.ValidateName("columnFamily", columnFamily);

            var cqlQuery = string.Format("select {2} from {0}.{1} {3};", this.KeySpace, columnFamily,
                    requestColumn == null || !requestColumn.Any() ? "*" : string.Join(", ", requestColumn),
                    this.GetLimitQuery(limit));
            var results = this.ExecuteQuery(cqlQuery);

            var objectList = this.TransformRowsAndDeserialize<T>(results, requestColumn);

            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetAllRows, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return objectList;
        }

        public virtual bool Delete(string key)
        {
            var st = DateTime.UtcNow;
            key = this.ValidateName("key", key);
            this.ExecuteNonQuery(string.Format("delete from {0}.{1} where key='{2}';", this.KeySpace, this.ColumnFamily, key));

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Delete, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return true;
        }

        public virtual bool AddIndex(string columnFamily, Dictionary<string, string> keyValues)
        {
            return this.AddIndex(columnFamily, keyValues, 0);
        }

        public virtual bool AddIndex(string columnFamily, Dictionary<string, string> keyValues, uint timeToLive)
        {
            try
            {
                var st = DateTime.UtcNow;
                CancellationTokenSource cts = new CancellationTokenSource();
                var session = this.GetCassandraSession();

                var statement =
                    this.CachePrepare(string.Format("insert into {0}.{1} (key, value) values (?, ?)", this.KeySpace,
                        columnFamily));
                var statementTtl =
                    this.CachePrepare(string.Format("insert into {0}.{1} (key, value) values (?, ?) USING TTL ?", this.KeySpace,
                        columnFamily));

                WaitUnlessFault(
                    keyValues.Select(
                        curData =>
                            timeToLive == 0
                                ? statement.Bind(BuildBindVars(columnFamily, curData.Key, curData.Value))
                                : statementTtl.Bind(BuildBindVars(columnFamily, curData.Key, curData.Value,
                                    (int) timeToLive)))
                        .Select(bind => session.ExecuteAsync(bind))
                        .Cast<Task>()
                        .ToArray(), cts.Token);

                //Record TimeTaken
                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Add, (int)(DateTime.UtcNow - st).TotalMilliseconds));

                return true;
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "AddIndex",
                    new object[] { columnFamily, keyValues, timeToLive },
                    ex);

                if (ex.Message.Contains("Unable to read data from the transport connection:") ||
                    ex.Message.Contains("No connection could be made because all servers have failed.") ||
                    ex.Message.Contains("Connection to Cassandra has timed out") || ex.Message.Contains("The task didn't complete before timeout"))
                {
                    //Record TimeTaken
                    if (this.graphProvider != null)
                        this.graphProvider.Record(new Point(Constants.CassandraConnectionFailed));

                    Thread.Sleep(1000);
                    return this.AddIndex(columnFamily, keyValues, timeToLive);
                }

                throw;
            }
        }

        public virtual bool AddIndex(string columnFamily, string key, string indexKey, string indexValue)
        {
            return this.AddIndexes(columnFamily, new List<Index>
            {
                new Index
                {
                    Name = key,
                    KeyValues = new Dictionary<string, string>
                    {
                        {
                            indexKey,
                            indexValue
                        }
                    }
                }
            }, 0);
        }

        public virtual bool AddIndex(string columnFamily, string key, string indexKey, string indexValue, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, new List<Index>
            {
                new Index
                {
                    Name = key,
                    KeyValues = new Dictionary<string, string>
                    {
                        {
                            indexKey,
                            indexValue
                        }
                    }
                }
            }, timeToLive);
        }

        public virtual bool AddIndexWithTimeUUID(string columnFamily, string key, string indexKey, string indexValue)
        {
            return this.AddIndexesWithTimeUUID(columnFamily, new List<Index>
            {
                new Index
                {
                    Name = key,
                    KeyValues = new Dictionary<string, string>
                    {
                        {
                            indexKey,
                            indexValue
                        }
                    }
                }
            }, 0);
        }

        public virtual bool AddIndexWithTimeUUID(string columnFamily, string key, string indexKey, string indexValue, uint timeToLive)
        {
            return this.AddIndexesWithTimeUUID(columnFamily, new List<Index>
            {
                new Index
                {
                    Name = key,
                    KeyValues = new Dictionary<string, string>
                    {
                        {
                            indexKey,
                            indexValue
                        }
                    }
                }
            }, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, Index index)
        {
            return this.AddIndexes(columnFamily, new List<Index> { index }, 0);
        }

        public virtual bool AddIndexes(string columnFamily, Index index, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, new List<Index> { index }, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, List<Index> indexes)
        {
            return this.AddIndexes(columnFamily, indexes, 0);
        }

        public virtual bool AddIndexes(string columnFamily, List<Index> indexes, uint timeToLive)
        {
            try
            {
                var st = DateTime.UtcNow;

                columnFamily = this.ValidateName("columnFamily", columnFamily);

                var session = this.GetCassandraSession();
                CancellationTokenSource cts = new CancellationTokenSource();

                var statement =
                   this.CachePrepare(string.Format("insert into {0}.{1} (key, column1, value) values (?, ?, ?)", this.KeySpace,
                       columnFamily));


                var tasks = new List<Task>();

                foreach (var keyItem in indexes)
                {
                    string key = this.ValidateNameNoException(keyItem.Name);
                    if (string.IsNullOrEmpty(key)) continue;

                    foreach (var item in keyItem.KeyValues)
                    {
                        var curColumnName = this.ValidateNameNoException(item.Key);
                        if (string.IsNullOrEmpty(curColumnName)) continue;

                        string curColumnValue = item.Value ?? "NULL";
                        BoundStatement bind;
                        if (timeToLive == 0 && keyItem.TimeToLive == 0)
                            bind = statement.Bind(BuildBindVars(columnFamily, key, curColumnName, curColumnValue));
                        else
                        {
                            var statementTtl = this.CachePrepare(string.Format("insert into {0}.{1} (key, column1, value) values (?, ?, ?) USING TTL {2}", this.KeySpace,
                                                                                         columnFamily, (int)(timeToLive + keyItem.TimeToLive)));

                            bind = statementTtl.Bind(BuildBindVars(columnFamily, key, curColumnName, curColumnValue));
                        }
                        var resultSetFuture = session.ExecuteAsync(bind);
                        tasks.Add(resultSetFuture);
                    }
                }
                WaitUnlessFault(tasks.ToArray(), cts.Token);
                
                //Record TimeTaken
                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_AddIndexes, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "AddIndexes",
                    new object[] { columnFamily, indexes, timeToLive },
                    ex);

                //Ignore the trigger excpetion : this exception is thrown by cassandra if index already exists
                if (ex.Message.Contains("java.lang.RuntimeException: Exception while creating trigger on CF with ID"))
                {
                    return true;
                }
                if (ex.Message.Contains("Unable to read data from the transport connection:") ||
                    ex.Message.Contains("No connection could be made because all servers have failed.") ||
                    ex.Message.Contains("Connection to Cassandra has timed out") ||
                    ex.Message.Contains("The task didn't complete before timeout") ||
                    ex.Message.Contains("Cassandra timeout during write query"))
                {
                    this.RecordConnectionFailed();
                }

                throw;
            }
            return true;
        }

        public virtual bool AddIndexesWithTimeUUID(string columnFamily, List<Index> indexes)
        {
            return this.AddIndexesWithTimeUUID(columnFamily, indexes, 0);
        }

        public virtual bool AddIndexesWithTimeUUID(string columnFamily, List<Index> indexes, uint timeToLive)
        {
            try
            {
                var st = DateTime.UtcNow;

                columnFamily = this.ValidateName("columnFamily", columnFamily);

                var session = this.GetCassandraSession();
                CancellationTokenSource cts = new CancellationTokenSource();

                var statement =
                   this.CachePrepare(string.Format("insert into {0}.{1} (key, column1, value, modification_timestamp) values (?, ?, ?, now())", this.KeySpace,
                       columnFamily));


                var tasks = new List<Task>();

                foreach (var keyItem in indexes)
                {
                    string key = this.ValidateNameNoException(keyItem.Name);
                    if (string.IsNullOrEmpty(key)) continue;

                    foreach (var item in keyItem.KeyValues)
                    {
                        var curColumnName = this.ValidateNameNoException(item.Key);
                        if (string.IsNullOrEmpty(curColumnName)) continue;

                        string curColumnValue = item.Value ?? "NULL";
                        BoundStatement bind;
                        if (timeToLive == 0 && keyItem.TimeToLive == 0)
                            bind = statement.Bind(BuildBindVars(columnFamily, key, curColumnName, curColumnValue));
                        else
                        {
                            var statementTtl = this.CachePrepare(string.Format("insert into {0}.{1} (key, column1, value, modification_timestamp) values (?, ?, ?, now()) USING TTL {2}", this.KeySpace,
                                                                                         columnFamily, (int)(timeToLive + keyItem.TimeToLive)));

                            bind = statementTtl.Bind(BuildBindVars(columnFamily, key, curColumnName, curColumnValue));
                        }
                        var resultSetFuture = session.ExecuteAsync(bind);
                        tasks.Add(resultSetFuture);
                    }
                }
                WaitUnlessFault(tasks.ToArray(), cts.Token);
                
                //Record TimeTaken
                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_AddIndexesWithTimeUUID, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "AddIndexesWithTimeUUID",
                    new object[] { columnFamily, indexes, timeToLive },
                    ex);

                //Ignore the trigger excpetion : this exception is thrown by cassandra if index already exists
                if (ex.Message.Contains("java.lang.RuntimeException: Exception while creating trigger on CF with ID"))
                {
                    return true;
                }
                if (ex.Message.Contains("Unable to read data from the transport connection:") ||
                    ex.Message.Contains("No connection could be made because all servers have failed.") ||
                    ex.Message.Contains("Connection to Cassandra has timed out") ||
                    ex.Message.Contains("The task didn't complete before timeout"))
                {
                    //Record TimeTaken
                    if (this.graphProvider != null)
                        this.graphProvider.Record(new Point(Constants.CassandraConnectionFailed));

                    Thread.Sleep(1000);
                    return this.AddIndexesWithTimeUUID(columnFamily, indexes, timeToLive);
                }

                throw;
            }
            return true;
        }

        public virtual bool AddIndexes(string columnFamily, TimeIndex index)
        {
            return this.AddIndexes(columnFamily, new List<TimeIndex> { index }, 0);
        }

        public virtual bool AddIndexes(string columnFamily, TimeIndex index, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, new List<TimeIndex> { index }, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, List<TimeIndex> indexes)
        {
            return this.AddIndexes(columnFamily, indexes, 0);
        }

        public virtual bool AddIndexes(string columnFamily, List<TimeIndex> indexes, uint timeToLive)
        {
            var uuidIndexes = indexes.Select(i => new TimeUUIDIndex
            {
                Name = i.Name,
                TimeToLive = i.TimeToLive,
                KeyValues = i.KeyValues.ToDictionary(t => this.GetTimeBasedGuid(t.Key),
                t => t.Value)
            }).ToList();

            return this.AddIndexes(columnFamily, uuidIndexes, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, TimeUUIDIndex index)
        {
            return this.AddIndexes(columnFamily, new List<TimeUUIDIndex> { index }, 0);
        }

        public virtual bool AddIndexes(string columnFamily, TimeUUIDIndex index, uint timeToLive)
        {
            return this.AddIndexes(columnFamily, new List<TimeUUIDIndex> { index }, timeToLive);
        }

        public virtual bool AddIndexes(string columnFamily, List<TimeUUIDIndex> indexes)
        {
            return this.AddIndexes(columnFamily, indexes, 0);
        }

        public virtual bool AddIndexes(string columnFamily, List<TimeUUIDIndex> indexes, uint timeToLive)
        {
            try
            {
                var st = DateTime.UtcNow;
                CancellationTokenSource cts = new CancellationTokenSource();
                var session = this.GetCassandraSession();
                var statement =
                   this.CachePrepare(string.Format("insert into {0}.{1} (key, column1, value) values (?, ?, ?)", this.KeySpace,
                       columnFamily));

                var tasks = new List<Task>();
                foreach (var keyItem in indexes)
                {
                    string key = this.ValidateNameNoException(keyItem.Name);
                    if (string.IsNullOrEmpty(key)) continue;

                    foreach (var item in keyItem.KeyValues)
                    {
                        var curColumnName = item.Key;
                        if (curColumnName == default(Guid)) continue;

                        string curColumnValue = item.Value ?? "NULL";
                        BoundStatement bind;
                        if (timeToLive == 0 && keyItem.TimeToLive == 0)
                            bind = statement.Bind(BuildBindVars(columnFamily, key, curColumnName, curColumnValue));
                        else
                        {
                            var statementTtl =
                                this.CachePrepare(string.Format("insert into {0}.{1} (key, column1, value) values (?, ?, ?) USING TTL {2}", this.KeySpace,
                                                          columnFamily, (int)(timeToLive + keyItem.TimeToLive)));
                            bind = statementTtl.Bind(BuildBindVars(columnFamily, key, curColumnName, curColumnValue));
                        }
                        var resultSetFuture = session.ExecuteAsync(bind);
                        tasks.Add(resultSetFuture);
                    }
                }
                WaitUnlessFault(tasks.ToArray(), cts.Token);
                
                //Record TimeTaken
                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_AddIndexes, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.Communicator<T>",
                    "AddIndexes",
                    new object[] { columnFamily, indexes, timeToLive },
                    ex);

                //Ignore the trigger excpetion : this exception is thrown by cassandra if index already exists
                if (ex.Message.Contains("java.lang.RuntimeException: Exception while creating trigger on CF with ID"))
                {
                    return true;
                }

                if (ex.Message.Contains("Unable to read data from the transport connection:") ||
                    ex.Message.Contains("No connection could be made because all servers have failed.") ||
                    ex.Message.Contains("Connection to Cassandra has timed out") ||
                    (ex.StackTrace.Contains("FluentCassandra.CassandraContext.SaveChanges(Boolean atomic)") && ex.Message.Contains("Object reference not set to an instance of an object")))
                {
                    //Record TimeTaken
                    if (this.graphProvider != null)
                        this.graphProvider.Record(new Point(Constants.CassandraConnectionFailed));
                }

                throw;
            }
            return true;
        }

        public virtual Dictionary<string, string> GetIndex(string columnFamily, string key)
        {
            return this.GetIndex(columnFamily, key, DefaultLimit);
        }

        [Obsolete("duplicate of Get index method")]
        public virtual Dictionary<string, string> GetUniqueIndex(string columnFamily, string key)
        {
            return this.GetUniqueIndex(columnFamily, key, DefaultLimit);
        }

        public virtual Dictionary<string, string> GetIndex(string columnFamily, string key, int limit)
        {
            var st = DateTime.UtcNow;
            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            IEnumerable<Row> results;
            if (this.UsePreparedStatementGetCalls)
            {
                results =
                    this.JoinedAsyncQuery(
                        string.Format("select column1,value from {0}.{1} where key=? {2};", this.KeySpace, columnFamily,
                            this.GetLimitQuery(limit)
                            ), new List<string>() {key});
            }
            else
            {
                results =
                    this.ExecuteQuery(string.Format("select column1,value from {0}.{1} where key='{2}' {3};",
                        this.KeySpace, columnFamily, key, this.GetLimitQuery(limit)));
            }
            var colVals = this.TransformRows<string>(results, "column1", "value");
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        [Obsolete("duplicate of Get index method")]
        public virtual Dictionary<string, string> GetUniqueIndex(string columnFamily, string key, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            IEnumerable<Row> results;
            if (this.UsePreparedStatementGetCalls)
            {
                results =
                    this.JoinedAsyncQuery(
                        string.Format("select column1,value from {0}.{1} where key=? {2};", this.KeySpace,
                            this.ColumnFamily,
                            this.GetLimitQuery(limit)), new List<string>() {key});
            }
            else
            {
                results =
                    this.ExecuteQuery(string.Format("select column1,value from {0}.{1} where key='{2}' {3};",
                        this.KeySpace, columnFamily, key, this.GetLimitQuery(limit)));
            }

            var colVals = this.TransformRows<string>(results, "column1", "value");
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetUniqueIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual Dictionary<string, string> GetIndexesColumns(string columnFamily)
        {
            return this.GetIndexesColumns(columnFamily, DefaultLimit);
        }

        public virtual Dictionary<string, string> GetIndexesColumns(string columnFamily, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);

            var results =
                this.ExecuteQuery(string.Format("select key, column1 from {0}.{1} {2};", this.KeySpace, columnFamily,
                    this.GetLimitQuery(limit)));

            Dictionary<string, string> colVals = null;

            if (!RemoveDuplicateData)
            {
                colVals = results.Where(
                        x =>
                            x != null && x.GetColumn("key") != null && x.GetColumn("column1") != null &&
                            (!string.IsNullOrEmpty(x.GetValue<string>("key")) && !x.IsNull("column1")))
                        .ToDictionary(row => row.GetValue<string>("key"), row => row.GetValue<string>("column1"));
            }
            else
            {
                colVals = results.Where(
                        x =>
                            x != null && x.GetColumn("key") != null && x.GetColumn("column1") != null &&
                            (!string.IsNullOrEmpty(x.GetValue<string>("key")) && !x.IsNull("column1")))
                        .GroupBy(row => row.GetValue<string>("key"))
                        .ToDictionary(y => y.Key, y => y.First().GetValue<string>("column1"));
            }

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndexesColumns, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<Index> GetIndexes(string columnFamily, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            var results =
                this.ExecuteQuery(string.Format("select key, column1,value from {0}.{1} {2};", this.KeySpace,
                    columnFamily, this.GetLimitQuery(limit)));
            var colVals =
                results.Where(x => x != null && x.GetColumn("column1") != null && !x.IsNull("column1"))
                    .Select(row => new Index
                    {
                        Name = row.GetValue<string>("key"),
                        KeyValues =
                            new Dictionary<string, string>
                            {
                                {row.GetValue<string>("column1"), row.GetValue<string>("value")}
                            },
                        TimeToLive = 0
                    }).ToList();

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndexes, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<Index> GetIndexes(string columnFamily, List<string> keys)
        {
            var st = DateTime.UtcNow;
            var results = this.JoinedAsyncQuery(
                String.Format(
                    "select key, column1,value from {0}.{1} where key=?", this.KeySpace, columnFamily),
                keys);

            var colVals = new Dictionary<string, Index>();

            foreach (var row in results)
            {
                var key = row.GetValue<string>("key");
                var column1 = row.GetValue<string>("column1");
                var value = row.GetValue<string>("value");

                Index curIndex;
                if (colVals.ContainsKey(key))
                    curIndex = colVals[key];
                else
                {
                    curIndex = new Index { Name = key, KeyValues = new Dictionary<string, string>(), TimeToLive = 0 };
                    colVals.Add(key, curIndex);
                }

                curIndex.KeyValues.Add(column1, value);
            }

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndexes, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals.Values.ToList();
        }

        public virtual List<string> GetIndexColumnNames(string columnFamily, string key)
        {
            return this.GetIndexColumnNames(columnFamily, key, DefaultLimit);
        }

        public virtual List<string> GetIndexColumnNames(string columnFamily, string key, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                results =
                    this.JoinedAsyncQuery(
                        string.Format("select column1 from {0}.{1} where key=? {2};", this.KeySpace, columnFamily,
                             this.GetLimitQuery(limit)), new List<string>() {key});
            }
            else
            {
                results =
                    this.ExecuteQuery(string.Format("select column1 from {0}.{1} where key='{2}' {3};", this.KeySpace,
                        columnFamily, key, this.GetLimitQuery(limit)));
            }
            var colVals = this.TransformRows(results, "column1");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndexColumnNames, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<string> GetIndexColumnValues(string columnFamily, string key)
        {
            return this.GetIndexColumnValues(columnFamily, key, DefaultLimit);
        }

        public virtual List<string> GetIndexColumnValues(string columnFamily, string key, int limit)
        {
            var st = DateTime.UtcNow;
            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                results =
                    this.JoinedAsyncQuery(
                        string.Format("select value from {0}.{1} where key=? {2};", this.KeySpace,
                            columnFamily, this.GetLimitQuery(limit)), new List<string>() {key});
            }
            else
            {
                 results =
                this.ExecuteQuery(string.Format("select value from {0}.{1} where key='{2}' {3};", this.KeySpace,
                    columnFamily, key, this.GetLimitQuery(limit)));
            }
            var colVals = this.TransformRows(results, "value");
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndexColumnValues, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual Dictionary<string, string> GetIndex(string columnFamily, string key, string indexKey)
        {
            return this.GetIndex(columnFamily, key, indexKey, DefaultLimit);
        }

        public virtual Dictionary<string, string> GetIndex(string columnFamily, string key, string indexKey, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
             IEnumerable<Row> results;

            if (this.UsePreparedStatement)
            {
                Dictionary<string, object> whereClause = new Dictionary<string, object>
                {
                    {"key", key},
                    {"column1", indexKey}
                };
                results = this.JoinedAsyncQuery(columnFamily, new List<string>() {"column1", "value"},
                    new Dictionary<int, Dictionary<string, object>> {{1, whereClause}}, limit, false);
            }
            else
            {
                results =
                    this.ExecuteQuery(
                        string.Format("select column1, value from {0}.{1} where key='{2}' and column1='{4}' {3};",
                            this.KeySpace, columnFamily, key, this.GetLimitQuery(limit), indexKey));
            }

            var colVals = this.TransformRows<string>(results, "column1", "value");
            
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        [Obsolete("duplicate of Get index method")]
        public virtual Dictionary<string, string> GetUniqueIndex(string columnFamily, string key, string indexKey, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);

            IEnumerable<Row> results;
            if (this.UsePreparedStatementGetCalls)
            {
                Dictionary<string, object> whereClause = new Dictionary<string, object>
                {
                    {"key", key},
                    {"column1", indexKey}
                };
                results = this.JoinedAsyncQuery(columnFamily, new List<string>() {"column1", "value"},
                    new Dictionary<int, Dictionary<string, object>> {{1, whereClause}}, limit, false);
            }
            else
            {
                results =
                this.ExecuteQuery(
                    string.Format("select column1, value from {0}.{1} where key='{2}' and column1='{4}' {3};",
                        this.KeySpace, columnFamily, key, this.GetLimitQuery(limit), indexKey));
            }

            
            var colVals = this.TransformRows<string>(results, "column1", "value");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetUniqueIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<string> GetIndexColumnNames(string columnFamily, string key, string indexKey)
        {
            return this.GetIndexColumnNames(columnFamily, key, indexKey, DefaultLimit);
        }

        public virtual List<string> GetIndexColumnNames(string columnFamily, string key, string indexKey, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);

            IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                Dictionary<string, object> whereClause = new Dictionary<string, object>
                {
                    {"key", key},
                    {"column1", indexKey}
                };
                results = this.JoinedAsyncQuery(columnFamily, new List<string>() {"column1"},
                    new Dictionary<int, Dictionary<string, object>> {{1, whereClause}}, limit, false);
            }
            else
            {
                results =
                this.ExecuteQuery(string.Format("select column1 from {0}.{1} where key='{2}' and column1='{4}' {3};",
                    this.KeySpace, columnFamily, key, this.GetLimitQuery(limit), indexKey));
            }
            var colVals = this.TransformRows(results, "column1");
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndexColumnNames, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<string> GetIndexColumnValues(string columnFamily, string key, string indexKey)
        {
            return this.GetIndexColumnValues(columnFamily, key, indexKey, DefaultLimit);
        }

        public virtual List<string> GetIndexColumnValues(string columnFamily, string key, string indexKey, int limit)
        {
            var st = DateTime.UtcNow;
            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);

            IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                Dictionary<string, object> whereClause = new Dictionary<string, object>
                {
                    {"key", key},
                    {"column1", indexKey}
                };
                 results = this.JoinedAsyncQuery(columnFamily, new List<string>() { "value" },
                 new Dictionary<int, Dictionary<string, object>> { { 1, whereClause } }, limit, false);
            }
            else
            {
                results =
                this.ExecuteQuery(string.Format("select value from {0}.{1} where key='{2}' and column1='{4}' {3};",
                    this.KeySpace, columnFamily, key, this.GetLimitQuery(limit), indexKey));
            }
            var colVals = this.TransformRows(results, "value");
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetIndexColumnValues, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string key, DateTime from, DateTime to)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetFromGuid(from), this.GetToGuid(to), DefaultLimit);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string key, DateTime from, DateTime to, int limit)
        {
            return this.GetTimeIndex(columnFamily, key, this.GetFromGuid(from), this.GetToGuid(to), limit);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string key, string from, string to)
        {
            return this.GetTimeIndex(columnFamily, key, from, to, DefaultLimit);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string key, string from, string to, int limit)
        {
            return this.GetTimeIndex(columnFamily, "column1", key, from, to, limit);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to)
        {
            return this.GetTimeIndex(columnFamily, columnName, key, from, to, DefaultLimit);
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to, int limit)
        {
            var st = DateTime.UtcNow;
            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            IEnumerable<Row> results;
            if (this.UsePreparedStatementGetCalls)
            {
                if (this.IsPreparedStatementRequiredForRangeQuery(columnFamily))
                {
                    var parititionKeys = new List<string>() { key };
                    var bindingParams = GetBindingParams(parititionKeys, from, to);
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select {2},value from {0}.{1} where key=? {3} {4};", this.KeySpace,
                                columnFamily, columnName, this.GetFromAndToParameterizedQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit)
                                ), parititionKeys, false, bindingParams);
                }
                else
                {
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select {2},value from {0}.{1} where key=? {3} {4};", this.KeySpace,
                                columnFamily, columnName, this.GetFromAndToQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit)
                                ), new List<string>() { key });
                }
            }
            else
            {
                results = this.ExecuteQuery(string.Format("select {2},value from {0}.{1} where key='{3}'{4} {5};", this.KeySpace,
                      columnFamily, columnName, key, this.GetFromAndToQuery(columnName, " AND ", from, to), this.GetLimitQuery(limit)));   
            }

            var colVals = this.TransformRows<Guid>(results, columnName, "value");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetTimeIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual Dictionary<string, string> GetTimeIndex(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);

            IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                if (this.IsPreparedStatementRequiredForRangeQuery(columnFamily))
                {
                    var parititionKeys = new List<string>() { key };
                    var bindingParams = GetBindingParams(parititionKeys, from, to);
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select {2}, value from {0}.{1} where key=? {3} order by {2} {5} {4};",
                                this.KeySpace, columnFamily, columnName,
                                this.GetFromAndToParameterizedQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit),
                                ascending ? "asc" : "desc"), parititionKeys, false, bindingParams);
                }
                else
                {
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select {2}, value from {0}.{1} where key=? {3} order by {2} {5} {4};",
                                this.KeySpace, columnFamily, columnName,
                                this.GetFromAndToQuery(columnName, " AND ", from, to), this.GetLimitQuery(limit),
                                ascending ? "asc" : "desc"), new List<string>() { key });
                }
            }
            else
            {
                results =
                    this.ExecuteQuery(
                        string.Format("select {2}, value from {0}.{1} where key='{3}'{4} order by {2} {6} {5};",
                            this.KeySpace, columnFamily, columnName, key,
                            this.GetFromAndToQuery(columnName, " AND ", from, to), this.GetLimitQuery(limit),
                            ascending ? "asc" : "desc"));
            }
            var colVals = this.TransformRows<Guid>(results, columnName, "value");
            
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetTimeIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string key, DateTime from, DateTime to)
        {
            return this.GetTimeIndexColumnNames(columnFamily, key, this.GetFromGuid(from), this.GetToGuid(to), DefaultLimit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string key, DateTime from, DateTime to, int limit)
        {
            return this.GetTimeIndexColumnNames(columnFamily, key, this.GetFromGuid(from), this.GetToGuid(to), limit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string key, string from, string to)
        {
            return this.GetTimeIndexColumnNames(columnFamily, key, from, to, DefaultLimit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string key, string from, string to, int limit)
        {
            return this.GetTimeIndexColumnNames(columnFamily, "column1", key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, DateTime from, DateTime to)
        {
            return this.GetTimeIndexColumnNames(columnFamily, columnName, key, this.GetFromGuid(from), this.GetToGuid(to), DefaultLimit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, DateTime from, DateTime to, int limit)
        {
            return this.GetTimeIndexColumnNames(columnFamily, columnName, key, this.GetFromGuid(from), this.GetToGuid(to), limit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to)
        {
            return this.GetTimeIndexColumnNames(columnFamily, columnName, key, from, to, DefaultLimit);
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);

            IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                if (this.IsPreparedStatementRequiredForRangeQuery(columnFamily))
                {
                    var parititionKeys = new List<string>() { key };
                    var bindingParams = GetBindingParams(parititionKeys, from, to);
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select {2} from {0}.{1} where key=? {3} {4};", this.KeySpace,
                                columnFamily, columnName,
                                this.GetFromAndToParameterizedQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit)), parititionKeys, false, bindingParams);
                }
                else
                {
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select {2} from {0}.{1} where key=? {3} {4};", this.KeySpace,
                                columnFamily, columnName, this.GetFromAndToQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit)), new List<string>() { key });
                }
            }
            else
            {
                results =
                    this.ExecuteQuery(string.Format("select {2} from {0}.{1} where key='{3}'{4} {5};", this.KeySpace,
                        columnFamily, columnName, key, this.GetFromAndToQuery(columnName, " AND ", from, to),
                        this.GetLimitQuery(limit)));
            }

            var colVals = this.TransformRows(results, columnName);

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetTimeIndexColumnNames, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<string> GetTimeIndexColumnNames(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                if (this.IsPreparedStatementRequiredForRangeQuery(columnFamily))
                {
                    var parititionKeys = new List<string>() { key };
                    var bindingParams = GetBindingParams(parititionKeys, from, to);
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select {2} from {0}.{1} where key=? {3} order by {2} {5} {4};",
                                this.KeySpace, columnFamily, columnName,
                                this.GetFromAndToParameterizedQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit), ascending ? "asc" : "desc"),
                                parititionKeys, false, bindingParams);
                }
                else
                {
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select {2} from {0}.{1} where key=? {3} order by {2} {5} {4};",
                                this.KeySpace, columnFamily, columnName,
                                this.GetFromAndToQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit), ascending ? "asc" : "desc"), new List<string>() { key });
                }
            }
            else
            {
                 results =
                this.ExecuteQuery(string.Format("select {2} from {0}.{1} where key='{3}'{4} order by {2} {6} {5};",
                    this.KeySpace, columnFamily, columnName, key, this.GetFromAndToQuery(columnName, " AND ", from, to),
                    this.GetLimitQuery(limit), ascending ? "asc" : "desc"));
            }
            var colVals = this.TransformRows(results, columnName);
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetTimeIndexColumnNames, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string key, DateTime from, DateTime to)
        {
            return this.GetTimeIndexColumnValues(columnFamily, key, this.GetFromGuid(from), this.GetToGuid(to), DefaultLimit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string key, DateTime from, DateTime to, int limit)
        {
            return this.GetTimeIndexColumnValues(columnFamily, key, this.GetFromGuid(from), this.GetToGuid(to), limit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string key, string from, string to)
        {
            return this.GetTimeIndexColumnValues(columnFamily, key, from, to, DefaultLimit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string key, string from, string to, int limit)
        {
            return this.GetTimeIndexColumnValues(columnFamily, "column1", key, from, to, limit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, DateTime from, DateTime to)
        {
            return this.GetTimeIndexColumnValues(columnFamily, columnName, key, this.GetFromGuid(from), this.GetToGuid(to), DefaultLimit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, DateTime from, DateTime to, int limit)
        {
            return this.GetTimeIndexColumnValues(columnFamily, columnName, key, this.GetFromGuid(from), this.GetToGuid(to), limit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to)
        {
            return this.GetTimeIndexColumnValues(columnFamily, columnName, key, from, to, DefaultLimit);
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                if (this.IsPreparedStatementRequiredForRangeQuery(columnFamily))
                {
                    var parititionKeys = new List<string>() { key };
                    var bindingParams = GetBindingParams(parititionKeys, from, to);
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select value from {0}.{1} where key=? {2} {3};",
                            this.KeySpace, columnFamily,
                            this.GetFromAndToParameterizedQuery(columnName, " AND ", from, to),
                            this.GetLimitQuery(limit)), parititionKeys, false, bindingParams);
                }
                else
                {
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select value from {0}.{1} where key=? {2} {3};", this.KeySpace,
                                columnFamily, this.GetFromAndToQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit)), new List<string>() { key });
                }
            }
            else
            {
                results =
                    this.ExecuteQuery(string.Format("select value from {0}.{1} where key='{2}'{3} {4};", this.KeySpace,
                        columnFamily, key, this.GetFromAndToQuery(columnName, " AND ", from, to),
                        this.GetLimitQuery(limit)));
            }


            var colVals = this.TransformRows(results, "value");
            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetTimeIndexColumnValues,
                                                                (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<string> GetTimeIndexColumnValues(string columnFamily, string columnName, string key, string from, string to, int limit, bool ascending)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                if (this.IsPreparedStatementRequiredForRangeQuery(columnFamily))
                {
                    var parititionKeys = new List<string>() { key };
                    var bindingParams = GetBindingParams(parititionKeys, from, to);
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select value from {0}.{1} where key=? {3} order by {2} {5} {4};",
                                this.KeySpace, columnFamily, columnName,
                                this.GetFromAndToParameterizedQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit),
                                ascending ? "asc" : "desc"), parititionKeys, false, bindingParams);
                }
                else
                {
                    results =
                        this.JoinedAsyncQuery(
                            string.Format("select value from {0}.{1} where key=? {3} order by {2} {5} {4};",
                                this.KeySpace, columnFamily, columnName,
                                this.GetFromAndToQuery(columnName, " AND ", from, to),
                                this.GetLimitQuery(limit),
                                ascending ? "asc" : "desc"), new List<string>() { key });
                }
            }
            else
            {
                results =
                    this.ExecuteQuery(string.Format(
                        "select value from {0}.{1} where key='{3}'{4} order by {2} {6} {5};",
                        this.KeySpace, columnFamily, columnName, key,
                        this.GetFromAndToQuery(columnName, " AND ", from, to),
                        this.GetLimitQuery(limit),
                        ascending ? "asc" : "desc"));
            }
            var colVals = this.TransformRows(results, "value");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetTimeIndexColumnValues,
                                                                (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        [Obsolete("Use the overload where keys is a list of strings!")]
        public virtual List<string> GetTimeIndexsColumnValues(string columnFamily, string columnName, string keys, string from, string to, int limit, bool ascending)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            keys = this.ValidateName("key", keys);

            var results =
                this.ExecuteQuery(string.Format("select value from {0}.{1} where key in {3}{4} order by {2} {6} {5};",
                    this.KeySpace, columnFamily, columnName, keys,
                    this.GetFromAndToQuery(columnName, " AND ", from, to), this.GetLimitQuery(limit),
                    ascending ? "asc" : "desc"));

            var colVals = this.TransformRows(results, "value"); 

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetTimeIndexColumnValues,
                                                                (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual List<string> GetTimeIndexsColumnValues(string columnFamily, string columnName, List<string> keys, string from, string to, int limit, bool ascending)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);

            IEnumerable<Row> results;
            if (this.IsPreparedStatementRequiredForRangeQuery(columnFamily))
            {
                var bindingParams = GetBindingParams(keys, from, to);
                results =
                    this.JoinedAsyncQuery(
                        string.Format("select value from {0}.{1} where key=? {3} order by {2} {5} {4};",
                        this.KeySpace, columnFamily, columnName,
                        this.GetFromAndToParameterizedQuery(columnName, " AND ", from, to),
                        this.GetLimitQuery(limit), ascending ? "asc" : "desc"), keys, false, bindingParams);
            }
            else
            {
                results =
                    this.JoinedAsyncQuery(
                        string.Format("select value from {0}.{1} where key=? {3} order by {2} {5} {4};", this.KeySpace,
                            columnFamily, columnName, this.GetFromAndToQuery(columnName, " AND ", from, to),
                            this.GetLimitQuery(limit),
                            ascending ? "asc" : "desc"), keys);
            }

            var colVals = this.TransformRows(results, "value");

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_GetTimeIndexColumnValues,
                                                                (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return colVals;
        }

        public virtual bool DeleteIndex(string columnFamily, string key)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);

            this.ExecuteNonQuery(string.Format("delete from {0}.{1} where key='{2}';", this.KeySpace, columnFamily, key));

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_DeleteIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return true;
        }

        public virtual bool DeleteIndex(string columnFamily, string key, string indexKey)
        {
            return this.DeleteIndex(columnFamily, "column1", key, indexKey);
        }

        public virtual bool DeleteIndex(string columnFamily, string columnName, string key, string indexKey)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            columnName = this.ValidateName("columnName", columnName);

            this.ExecuteNonQuery(string.Format("delete from {0}.{1} where key='{2}' and {3}='{4}';", this.KeySpace, columnFamily, key, columnName, indexKey));

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_DeleteIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return true;
        }

        public virtual bool DeleteTimeIndex(string columnFamily, string key, string indexKey)
        {
            return this.DeleteTimeIndex(columnFamily, "column1", key, indexKey);
        }

        public virtual bool DeleteTimeIndex(string columnFamily, string columnName, string key, string indexKey)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
            columnName = this.ValidateName("columnName", columnName);

            this.ExecuteNonQuery(string.Format("delete from {0}.{1} where key='{2}' and {3}={4};", this.KeySpace, columnFamily, key, columnName, indexKey));

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_DeleteTimeIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            return true;
        }

        public virtual int Count(string columnFamily, string key)
        {
            return this.Count(columnFamily, key, DefaultLimit);
        }

        public virtual int Count(string columnFamily, string key, int limit)
        {
            var st = DateTime.UtcNow;

            columnFamily = this.ValidateName("columnFamily", columnFamily);
            key = this.ValidateName("key", key);
             IEnumerable<Row> results;
            if (this.UsePreparedStatement)
            {
                results =
                    this.JoinedAsyncQuery(
                        string.Format("select count(*) from {0}.{1} where key=? {2};", this.KeySpace,
                            columnFamily, this.GetLimitQuery(limit)), new List<string>() {key});
            }
            else
            {
                results =
                    this.ExecuteQuery(string.Format("select count(*) from {0}.{1} where key='{2}' {3};", this.KeySpace,
                        columnFamily, key, this.GetLimitQuery(limit)));
            }

            //Record TimeTaken
            if (this.graphProvider != null)
                this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_Count, (int)(DateTime.UtcNow - st).TotalMilliseconds));

            if (results.Count() == 1)
                return int.Parse(results.First().GetValue<string>("count"));

            return 0;
        }

        public virtual Guid GetTimeBasedGuid(DateTime dt)
        {
            return TimeUuid.NewId(dt);
        }
        
        public virtual List<T> TransformRowsAndDeserialize<T>(IEnumerable<Row> rows, string desiredColumn)
        {
            return rows.Where(x => x != null && x.GetColumn(desiredColumn) != null && !x.IsNull(desiredColumn))
                .Select(row => JSon.Deserialize<T>(row.GetValue<string>(desiredColumn))).ToList();
        }

        public virtual List<T> TransformRowsAndDeserialize<T>(IEnumerable<Row> rows, List<string> desiredColumns)
        {
            var records = new List<T>();
            foreach (var row in rows)
            {
                var rowData = new Dictionary<string, string>();
                foreach (var column in desiredColumns)
                {
                    if (row != null && row.GetColumn(column) != null && !row.IsNull(column))
                    {
                        rowData.Add(column, row.GetValue<object>(column).ToString());
                    }
                }

                records.Add(JSon.Deserialize<T>(JSon.Serialize(rowData)));
            }

            return records;
        }

        public virtual List<string> TransformRows(IEnumerable<Row> rows, string desiredColumn)
        {
            return rows
                .Where(x => x != null && x.GetColumn(desiredColumn) != null && !x.IsNull(desiredColumn))
                .Select(row => row.GetValue<string>(desiredColumn)).ToList();
        }

        public virtual Dictionary<string, string> TransformRows<TColumnType>(IEnumerable<Row> rows, string keyColumn, string valueColumn)
        {
            if (!RemoveDuplicateData)
            {
                return rows.Where(x => x != null && x.GetColumn(keyColumn) != null && x.GetColumn(valueColumn) != null
                                 && (!x.IsNull(keyColumn) || !x.IsNull(valueColumn)))
                        .ToDictionary(row => row.GetValue<TColumnType>(keyColumn).ToString(), row => row.GetValue<string>(valueColumn));
            }
            else
            {
                return rows.Where(x => x != null && x.GetColumn(keyColumn) != null && x.GetColumn(valueColumn) != null
                                                 && (!x.IsNull(keyColumn) || !x.IsNull(valueColumn)))
                        .GroupBy(row => row.GetValue<TColumnType>(keyColumn).ToString())
                        .ToDictionary(y => y.Key, y => y.First().GetValue<string>(valueColumn));
            }
        }


        public virtual DateTime GetDateFromGuid(Guid guid)
        {
            return ((TimeUuid)guid).GetDate().DateTime;
        }

        public virtual ISession GetCassandraSession()
        {
            return DSEConnection.Session;
        }

        public virtual IMapper GetMapper()
        {
            var session = this.GetCassandraSession();
            return new Mapper(session);
        }

        private string GetLimitQuery(int limit)
        {
            return limit > 0 ? string.Format("limit {0}", limit) : string.Empty;
        }

        private string GetFromGuid(DateTime from)
        {
            return from != DateTime.MinValue ? this.GetTimeBasedGuid(from).ToString() : "";
        }

        private string GetToGuid(DateTime to)
        {
            return to != DateTime.MaxValue ? this.GetTimeBasedGuid(to).ToString() : "";
        }

        private string GetFromAndToQuery(string columnName, string prefix, DateTime from, DateTime to)
        {
            return this.GetFromAndToQuery(columnName, prefix, this.GetFromGuid(from), this.GetToGuid(to));
        }

        private string GetFromAndToQuery(string columnName, string prefix, string from, string to)
        {
            var queries = new List<string>();
            if (!string.IsNullOrEmpty(from))
                queries.Add(columnName + " >= " + from);
            if (!string.IsNullOrEmpty(to))
                queries.Add(columnName + " <= " + to);

            if (queries.Count == 0)
                return "";

            return prefix + string.Join(" AND ", queries);
        }

        private string GetFromAndToParameterizedQuery(string columnName, string prefix, string from, string to)
        {
            var queries = new List<string>();
            if (!string.IsNullOrWhiteSpace(from))
                queries.Add(columnName + " >= ?");
            if (!string.IsNullOrWhiteSpace(to))
                queries.Add(columnName + " <= ?");

            if (queries.Count == 0)
                return "";

            return prefix + string.Join(" AND ", queries);
        }

        public static void WaitUnlessFault(Task[] tasks, CancellationToken token)
        {
            var cts = CancellationTokenSource.CreateLinkedTokenSource(token);

            foreach (var task in tasks)
            {
                task.ContinueWith(t =>
                {
                    if (t.IsFaulted) cts.Cancel();
                },
                cts.Token,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Current);
            }

            try
            {
                Task.WaitAll(tasks, cts.Token);
            }
            catch (OperationCanceledException ex)
            {
                var faultedTask = tasks.FirstOrDefault(t => t.IsFaulted);

                if (faultedTask != null)
                {
                    var faultedTaskEx = faultedTask.Exception;

                    Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v1.Cassandra.DSEDataExplorer",
                    "FaultedTask",
                    new object[] { faultedTask, faultedTaskEx },
                    ex);

                    throw faultedTaskEx;
                }
                var canceledTask = tasks.FirstOrDefault(c => c.IsCanceled);

                if (canceledTask != null)
                {
                    var canceledTaskEx = canceledTask.Exception;

                    Trace.Exception(TraceTypes.AccessLayer,
                        "automation.components.data.v1.Cassandra.DSEDataExplorer",
                        "CanceledTask",
                        new object[] { canceledTask, canceledTaskEx },
                        ex);

                    throw canceledTaskEx;
                }
            }
        }

        private PreparedStatement CachePrepare(string query)
        {
            PreparedStatement statement;
            if (preparedStatementCache.TryGetValue(query, out statement))
            {
                return statement;
            }

            // Race condition is possible here, but it's not the end of the world
            statement = this.GetCassandraSession().Prepare(query);
            preparedStatementCache[query] = statement;
            return statement;
        }

        private string ValidateName(string key, string value)
        {
            if (string.IsNullOrEmpty(value)) throw new NullReferenceException(key);
            return value.Replace("'", "''");
        }

        private string ValidateNameNoException(string value)
        {
            if (string.IsNullOrEmpty(value)) return null;
            return value.Replace("'", "''");
        }


        /// <summary>
        /// This is needed to replace null items with Unset.Value to prevent excessive tombstones
        /// https://docs.datastax.com/en/developer/csharp-driver/3.0/features/datatypes/nulls-unset/
        /// Credit: Benjamin Trent
        /// </summary>
        /// <param name="values">Values to transform</param>
        /// <returns>Values with null replaced with Unset.Value</returns>
        private static object[] BuildBindVars(params object[] values)
        {
            if (!Parser.ToBoolean(Manager.GetApplicationConfigValue("RBAN-7824", "AllApplications.Cassandra"), false))
            {
                return values;
            }

            for (int i = 0; i < values.Length; i++)
            {
                // Checks if null ref or a serialized JSON "null" string
                if (values[i] == null || (values[i] is string && string.Equals((string)values[i], "null", StringComparison.CurrentCultureIgnoreCase)))
                {
                    values[i] = Unset.Value;
                }
            }

            return values;
        }

        private static object[] BuildBindVars(string columnFamily, params object[] values)
        {
            var columnFamilies = Manager.GetApplicationConfigValue("RBAN-8250", "AllApplications.Cassandra").Split(',');
            return columnFamilies.Any(x => x != null && x.Equals(columnFamily)) ? BuildBindVars(values) : values;
        }

        private IEnumerable<Row> ExecuteQuery(string query, bool executeInSingleNode = false)
        {
            var st = DateTime.UtcNow;

            try
            {
                var session = this.GetCassandraSession();
                return executeInSingleNode ? session.Execute(query, ConsistencyLevel.LocalOne).GetRows() : session.Execute(query).GetRows();
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "ExecuteQuery",
                    new object[] { query },
                    ex);

                if (ex.Message.Contains("Unable to read data from the transport connection:") ||
                    ex.Message.Contains("No connection could be made because all servers have failed.") ||
                    ex.Message.Contains("Connection to Cassandra has timed out") ||
                    ex.Message.Contains("Cassandra timeout during read query"))
                {
                    this.RecordConnectionFailed();
                }
                throw;
            }
            finally
            {
                this.ReportSlowQueries("GetColumnFamily", query, st);

                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_ExecuteQuery, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
        }

        [Obsolete("use other ExecuteQuery method with request columns as parameters")]
        public List<T> ExecuteQuery<T>(string query)
        {
            var st = DateTime.UtcNow;

            try
            {
                IMapper mapper = this.GetMapper();
                return mapper.Fetch<T>(query).ToList();
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "ExecuteQuery<T>",
                    new object[] { query },
                    ex);

                if (ex.Message.Contains("Unable to read data from the transport connection:") ||
                    ex.Message.Contains("No connection could be made because all servers have failed.") ||
                    ex.Message.Contains("Connection to Cassandra has timed out"))
                {
                    //Record TimeTaken
                    if (this.graphProvider != null)
                        this.graphProvider.Record(new Point(Constants.CassandraConnectionFailed));
                }
                throw;
            }
            finally
            {
                this.ReportSlowQueries("ExecuteQuery<T>", query, st);

                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_ExecuteQuery, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
        }


        public List<T> ExecuteQueryForSOLR<T>(string columnFamily, List<string> requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit)
        {
            return ExecuteQueryCommon<T>(columnFamily, requestColumn, whereClause, limit, false, false, true);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="columnFamily"></param>
        /// <param name="requestColumn"></param>
        /// <param name="whereClause"></param>
        /// <param name="limit"></param>
        /// <param name="executeWithCLOne"></param>
        /// <param name="executeWithCLLocalQuorum"></param>
        /// <param name="executeWithCLLocalOne"></param>
        /// <returns></returns>
        public List<T> ExecuteQueryToGetOriginalDataRows<T>(string columnFamily, List<string> requestColumn, Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne = false, bool executeWithCLLocalQuorum = false, bool executeWithCLLocalOne = false)
        {
            if (whereClause == null || !whereClause.Any())
            {
                return default(List<T>);
            }

            var results = this.JoinedAsyncQuery(columnFamily, requestColumn, whereClause, limit, false, executeWithCLOne, executeWithCLLocalQuorum, executeWithCLLocalOne);

            var response = results != null && results.Count > 0
                ? results.Where(x => x != null)
                    .Select(x => JSon.Deserialize<T>(x.ToString()))
                    .ToList()
                : default(List<T>);
            return response;
        }

        public List<T> ExecuteQuery<T>(string columnFamily, List<string> requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne = false,
            bool executeWithCLLocalQuorum = false)
        {
            return ExecuteQueryCommon<T>(columnFamily, requestColumn, whereClause, limit, executeWithCLOne, executeWithCLLocalQuorum);
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
            return ExecuteQueryCommon<T>(columnFamily, requestColumn, whereClause, timeStampColumnName, start, end, limit, executeWithCLOne, executeWithCLLocalQuorum);
        }

        private List<T> ExecuteQueryCommon<T>(string columnFamily, List<string> requestColumn, 
            Dictionary<int, Dictionary<string, object>> whereClause, int limit, 
            bool executeWithCLOne = false, bool executeWithCLLocalQuorum = false, bool executeWithCLLocalOne = false)
        {
            if (whereClause == null || !whereClause.Any())
                return default(List<T>);

            var results = this.JoinedAsyncQuery(columnFamily, requestColumn, whereClause, limit, true, executeWithCLOne,
                executeWithCLLocalQuorum, executeWithCLLocalOne);

            var response = results != null && results.Count > 0
                ? results.Where(x => x != null && x.GetColumn("[json]") != null)
                    .Select(x => JSon.Deserialize<T>(x.GetValue<string>("[json]")))
                    .ToList()
                : default(List<T>);
            return response;
        }

        private List<T> ExecuteQueryCommon<T>(string columnFamily, List<string> requestColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, string timeStampColumnName, DateTime start, DateTime end, int limit,
            bool executeWithCLOne = false, bool executeWithCLLocalQuorum = false, bool executeWithCLLocalOne = false)
        {
            if (whereClause == null || !whereClause.Any())
                return default(List<T>);

            var results = this.JoinedAsyncQuery(columnFamily, requestColumn, whereClause, timeStampColumnName, start, end, limit, true, executeWithCLOne,
                executeWithCLLocalQuorum, executeWithCLLocalOne);

            var response = results != null && results.Count > 0
                ? results.Where(x => x != null && x.GetColumn("[json]") != null)
                    .Select(x => JSon.Deserialize<T>(x.GetValue<string>("[json]")))
                    .ToList()
                : default(List<T>);
            return response;
        }

        public List<string> GetColumnValues(string columnFamily, string requestColumn, Dictionary<int, Dictionary<string, object>> whereClause, int limit, bool executeWithCLOne,
            bool executeWithCLLocalQuorum)
        {
             if (whereClause == null || !whereClause.Any())
                return default(List<string>);

            var rows = this.JoinedAsyncQuery(columnFamily, new List<string>() {requestColumn}, whereClause, limit,
                false, executeWithCLOne, executeWithCLLocalQuorum, false);
            return this.TransformRows(rows, requestColumn);
        }

        public bool ExecuteNonQuery(string columnFamily, List<RowIndex> indexes)
        {
            return this.ExecuteNonQuery(columnFamily, indexes, 0);
        }

        public bool ExecuteNonQuery(string columnFamily, List<RowIndex> indexes, uint timeToLive)
        {
            try
            {
                var st = DateTime.UtcNow;
                CancellationTokenSource cts = new CancellationTokenSource();
                var session = this.GetCassandraSession();

                var tasks = new List<Task>();
                foreach (var row in indexes)
                {
                    var items = Enumerable.Repeat("?", row.KeyValues.Keys.Count).ToArray();
                    string cqlQuery;
                    if (timeToLive == 0 && row.TimeToLive == 0)
                    {
                        cqlQuery = string.Format("insert into {0}.{1} ({2}{4}{6}) values ({3}{5}{7})", this.KeySpace,
                            columnFamily, string.Join(", ", row.KeyValues.Keys), string.Join(", ", items),
                            row.EnableAutoTimeUUID ? ", modification_timestamp" : string.Empty,
                            row.EnableAutoTimeUUID ? ", now()" : string.Empty,
                            row.EnableAutoCreationTimeUUID ? ", entry_timestamp" : string.Empty,
                            row.EnableAutoCreationTimeUUID ? ", now()" : string.Empty);
                    }
                    else
                    {
                        cqlQuery = string.Format("insert into {0}.{1} ({2}{4}{7}) values ({3}{5}{8}) USING TTL {6}",
                            this.KeySpace, columnFamily, string.Join(", ", row.KeyValues.Keys),
                            string.Join(", ", items),
                            row.EnableAutoTimeUUID ? ", modification_timestamp" : string.Empty,
                            row.EnableAutoTimeUUID ? ", now()" : string.Empty,
                            (int) (timeToLive + row.TimeToLive),
                            row.EnableAutoCreationTimeUUID ? ", entry_timestamp" : string.Empty,
                            row.EnableAutoCreationTimeUUID ? ", now()" : string.Empty);
                    }

                    var statement = this.CachePrepare(cqlQuery);
                    BoundStatement bind = statement.Bind(BuildBindVars(columnFamily, row.KeyValues.Select(x => x.Value).ToArray()));
                    var resultSetFuture = session.ExecuteAsync(bind);
                    tasks.Add(resultSetFuture);
                }
                WaitUnlessFault(tasks.ToArray(), cts.Token);

                //Record TimeTaken
                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_ExecuteNonQuery, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.Communicator<T>",
                    "ExecuteNonQuery",
                    new object[] { columnFamily, indexes, timeToLive },
                    ex);

                //Ignore the trigger excpetion : this exception is thrown by cassandra if index already exists
                if (ex.Message.Contains("java.lang.RuntimeException: Exception while creating trigger on CF with ID"))
                {
                    return true;
                }

                if (ex.Message.Contains("Unable to read data from the transport connection:") ||
                    ex.Message.Contains("No connection could be made because all servers have failed.") ||
                    ex.Message.Contains("Connection to Cassandra has timed out") ||
                    (ex.StackTrace.Contains("FluentCassandra.CassandraContext.SaveChanges(Boolean atomic)") && ex.Message.Contains("Object reference not set to an instance of an object")))
                {
                    //Record TimeTaken
                    if (this.graphProvider != null)
                        this.graphProvider.Record(new Point(Constants.CassandraConnectionFailed));
                }

                throw;
            }
            return true;
        }

        private void ExecuteNonQuery(string query)
        {
            var st = DateTime.UtcNow;
            try
            {
                var session = this.GetCassandraSession();
                session.Execute(query);
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "ExecuteNonQuery",
                    new object[] { query },
                    ex);

                if (ex.Message.Contains("Unable to read data from the transport connection:") ||
                    ex.Message.Contains("No connection could be made because all servers have failed.") ||
                    ex.Message.Contains("Connection to Cassandra has timed out"))
                {
                    //Record TimeTaken
                    if (this.graphProvider != null)
                        this.graphProvider.Record(new Point(Constants.CassandraConnectionFailed));

                }
                throw;
            }
            finally
            {
                this.ReportSlowQueries("GetColumnFamily", query, st);

                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_ExecuteNonQuery, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
        }

        private void ExecuteNonQuery(BoundStatement statement)
        {
            var st = DateTime.UtcNow;
            try
            {
                var session = this.GetCassandraSession();
                session.Execute(statement);
            }
            catch (Exception ex)
            {
                Trace.Exception(
                    TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "ExecuteNonQuery",
                    new object[] { statement },
                    ex);
                throw;
            }
            finally
            {
                if (this.graphProvider != null)
                    this.graphProvider.RecordTimeTaken(new Point(Constants.CassLatency_ExecuteNonQuery, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }
        }

        private void ReportSlowQueries(string methodName, string query, DateTime requestTime)
        {
            this.ReportSlowQueries(methodName, query, requestTime, DateTime.UtcNow);
        }

        private void ReportSlowQueries(string methodName, string query, DateTime requestTime, DateTime responseTime)
        {
            var tt = responseTime - requestTime;

            if (tt.TotalSeconds < 2)
                return;

            try
            {
                Trace.Warning(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    methodName,
                    new object[] { "Possible slow query detected",
                                new Dictionary<string, dynamic> {
                                                                    { "Query", query },
                                                                    { "Time Taken", tt },
                                                                    { "Request Time", requestTime },
                                                                    { "Response Time", responseTime }
                                                                }
                            });
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,"automation.components.data.v2.Cassandra.DSEDataExplorer<T>","ReportSlowQueries", new object[] { query }, ex);
            }
        }

        private void RecordConnectionFailed()
        {
            try
            {
                if (this.graphProvider != null)
                    this.graphProvider.Record(new Point(string.Format("{0}.{1}", Environment.MachineName, Constants.CassandraConnectionFailed)));
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer, "automation.components.data.v2.Cassandra.DSEDataExplorer<T>", "RecordConnectionFailed", new object[] { }, ex);
            }
        }

        private void RecordConnectionFailed(string errorMessage)
        {
            try
            {
                if (errorMessage.Contains("Unable to read data from the transport connection:") ||
                    errorMessage.Contains("No connection could be made because all servers have failed.") ||
                    errorMessage.Contains("Connection to Cassandra has timed out") ||
                    errorMessage.Contains("The task didn't complete before timeout") ||
                    errorMessage.Contains("Cassandra timeout during write query"))
                {
                    if (this.graphProvider != null)
                        this.graphProvider.Record(new Point(string.Format("{0}.{1}", Environment.MachineName, Constants.CassandraConnectionFailed)));
                }

            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer, "automation.components.data.v2.Cassandra.DSEDataExplorer<T>", "RecordConnectionFailed", new object[] { errorMessage }, ex);
            }
        }

        private List<Row> JoinedAsyncQuery(string query, List<string> partitionKeys, bool executeWithCLOne = false, List<object> bindingParams = null)
        {
            var cts = new CancellationTokenSource();
            var results = new HashSet<Row>();
            var futures = new List<Task<RowSet>>(partitionKeys.Count);

            var session = this.GetCassandraSession();
            var statement = this.CachePrepare(query);

            foreach (var partitionKey in partitionKeys.Distinct())
            {
                if (string.IsNullOrWhiteSpace(partitionKey))
                    continue;

                IStatement bind = statement.Bind(partitionKey);
                if (bindingParams != null)
                    bind = statement.Bind(bindingParams.ToArray());

                if (executeWithCLOne)
                    bind = bind.SetConsistencyLevel(ConsistencyLevel.One);

                futures.Add(session.ExecuteAsync(bind));
            }

            try
            {
                WaitUnlessFault(futures.ToArray(), cts.Token);
            }
            catch (ReadTimeoutException readTimeoutException)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "JoinedAsyncQuery",
                    new object[] { query },
                    readTimeoutException);

                this.RecordConnectionFailed();
                throw;
            }

            // Flatten and uniquify the results
            results.UnionWith(futures.Select(x => x.Result).SelectMany(x => x));

            return results.ToList();
        }

        private List<Row> JoinedAsyncQuery(string columnFamily, List<string> returnColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, int limit,bool selectJson, bool executeWithCLOne = false,
            bool executeWithCLLocalQuorum = false, bool executeWithCLLocalOne = false)
        {
            var cts = new CancellationTokenSource();
            var results = new HashSet<Row>();
            var futures = new List<Task<RowSet>>(whereClause.Count);

            var session = this.GetCassandraSession();

            foreach (var columns in whereClause.Values)
            {
                if (columns == null || !columns.Any())
                    continue;

                string cqlQuery;
                if (selectJson)
                {
                    cqlQuery = string.Format("select JSON {2} from {0}.{1} where {3} {4};", this.KeySpace, columnFamily,
                        returnColumn == null || !returnColumn.Any() ? "*" : string.Join(", ", returnColumn),
                        string.Join(" and ", columns.Select(x => string.Format("{0} = ?", x.Key, x.Value))),
                        this.GetLimitQuery(limit));
                }
                else
                {
                    cqlQuery = string.Format("select {2} from {0}.{1} where {3} {4};", this.KeySpace, columnFamily,
                        returnColumn == null || !returnColumn.Any() ? "*" : string.Join(", ", returnColumn),
                        string.Join(" and ", columns.Select(x => string.Format("{0} = ?", x.Key, x.Value))),
                        this.GetLimitQuery(limit));
                }

                var statement = this.CachePrepare(cqlQuery);
                IStatement bind = statement.Bind(columns.Select(x => x.Value).ToArray());

                if (executeWithCLLocalQuorum)
                    bind = bind.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                else if (executeWithCLOne)
                    bind = bind.SetConsistencyLevel(ConsistencyLevel.One);
                else if (executeWithCLLocalOne)
                    bind = bind.SetConsistencyLevel(ConsistencyLevel.LocalOne);

                futures.Add(session.ExecuteAsync(bind));
            }

            try
            {
                WaitUnlessFault(futures.ToArray(), cts.Token);
            }
            catch (ReadTimeoutException readTimeoutException)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "JoinedAsyncQuery",
                    new object[]
                    {columnFamily, returnColumn, whereClause, limit, executeWithCLOne, executeWithCLLocalQuorum},
                    readTimeoutException);

                this.RecordConnectionFailed();
                throw;
            }

            // Flatten and uniquify the results
            results.UnionWith(futures.Select(x => x.Result).SelectMany(x => x));

            return results.ToList();
        }

        private List<Row> JoinedAsyncQuery(string columnFamily, List<string> returnColumn,
            Dictionary<int, Dictionary<string, object>> whereClause, string timeStampColumnName, DateTime start, DateTime end, int limit, bool selectJson, bool executeWithCLOne = false,
            bool executeWithCLLocalQuorum = false, bool executeWithCLLocalOne = false)
        {
            var cts = new CancellationTokenSource();
            var results = new HashSet<Row>();
            var futures = new List<Task<RowSet>>(whereClause.Count);

            var session = this.GetCassandraSession();

            foreach (var columns in whereClause.Values)
            {
                if (columns == null || !columns.Any())
                    continue;

                string cqlQuery;
                if (selectJson)
                {
                    cqlQuery = string.Format("select JSON {2} from {0}.{1} where {3} and {5} {4};", this.KeySpace, columnFamily,
                        returnColumn == null || !returnColumn.Any() ? "*" : string.Join(", ", returnColumn),
                        string.Join(" and ", columns.Select(x => string.Format("{0} = ?", x.Key, x.Value))),
                        this.GetLimitQuery(limit), this.GetFromAndToQuery(timeStampColumnName, String.Empty, start, end));
                }
                else
                {
                    cqlQuery = string.Format("select {2} from {0}.{1} where {3} {4};", this.KeySpace, columnFamily,
                        returnColumn == null || !returnColumn.Any() ? "*" : string.Join(", ", returnColumn),
                        string.Join(" and ", columns.Select(x => string.Format("{0} = ?", x.Key, x.Value))),
                        this.GetLimitQuery(limit), this.GetFromAndToQuery(timeStampColumnName, " AND ", start, end));
                }

                var statement = this.CachePrepare(cqlQuery);
                IStatement bind = statement.Bind(columns.Select(x => x.Value).ToArray());

                if (executeWithCLLocalQuorum)
                    bind = bind.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                else if (executeWithCLOne)
                    bind = bind.SetConsistencyLevel(ConsistencyLevel.One);
                else if (executeWithCLLocalOne)
                    bind = bind.SetConsistencyLevel(ConsistencyLevel.LocalOne);

                futures.Add(session.ExecuteAsync(bind));
            }

            try
            {
                WaitUnlessFault(futures.ToArray(), cts.Token);
            }
            catch (ReadTimeoutException readTimeoutException)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v2.Cassandra.DSEDataExplorer<T>",
                    "JoinedAsyncQuery",
                    new object[] { columnFamily, returnColumn, whereClause, limit, executeWithCLOne, executeWithCLLocalQuorum },
                    readTimeoutException);

                this.RecordConnectionFailed();
                throw;
            }

            // Flatten and uniquify the results
            results.UnionWith(futures.Select(x => x.Result).SelectMany(x => x));

            return results.ToList();
        }

        private Dictionary<string, object> TransformRows(IEnumerable<Row> rows, string keyColumn, string valueColumn)
        {
            if (!RemoveDuplicateData)
            {
                return rows.Where(x => x != null && x.GetColumn(keyColumn) != null && x.GetColumn(valueColumn) != null
                                                 && (!x.IsNull(keyColumn) || !x.IsNull(valueColumn)))
                        .ToDictionary(row => row.GetValue<string>(keyColumn).ToString(), row => row.GetValue<object>(valueColumn));
            }
            else
            {
                return rows.Where(x => x != null && x.GetColumn(keyColumn) != null && x.GetColumn(valueColumn) != null
                                                 && (!x.IsNull(keyColumn) || !x.IsNull(valueColumn)))
                        .GroupBy(row => row.GetValue<string>(keyColumn).ToString())
                        .ToDictionary(y => y.Key, y => y.First().GetValue<object>(valueColumn));
            }
        }

        public static List<object> GetBindingParams(List<string> keys, string from, string to)
        {
            var bindingParams = new List<object>();
            bindingParams.AddRange(keys);
            if (!string.IsNullOrWhiteSpace(from))
                bindingParams.Add(new Guid(from));

            if (!string.IsNullOrWhiteSpace(to))
                bindingParams.Add(new Guid(to));

            return bindingParams;
        }

        private bool IsPreparedStatementRequiredForRangeQuery(string columnFamily)
        {
            return Parser.ToBoolean(Manager.GetApplicationConfigValue("RBAN-11755", "FeatureFlag"), false) &&
                Parser.ToString(Manager.GetApplicationConfigValue("RBAN-11755", "AllApplications.Cassandra"), string.Empty)
                .Split(',').Contains(columnFamily);
        }

        public virtual void Dispose()
        {

        }

        // Depricated Methods
        private const string DeprecationMessage = "[ARIC-6571] Retries are now handled via RetryPolicy";

        [Obsolete(DeprecationMessage)]
        public virtual T Get<T>(string key, int retryCount, bool resetDBConnection)
        {
            return this.Get<T>(key);
        }

        [Obsolete(DeprecationMessage)]
        public virtual T Get<T>(string columnFamily, string key, int retryCount, bool resetDBConnection)
        {
            return this.Get<T>(columnFamily, key);
        }

        [Obsolete(DeprecationMessage)]
        public virtual List<T> Get<T>(List<string> keys, int retryCount, bool resetDBConnection)
        {
            return this.Get<T>(keys);
        }

        [Obsolete(DeprecationMessage)]
        public virtual List<T> Get<T>(string columnFamily, List<string> keys, int retryCount, bool resetDBConnection)
        {
            return this.Get<T>(columnFamily, keys);
        }

        [Obsolete(DeprecationMessage)]
        public virtual bool AddIndexes(string columnFamily, List<Index> indexes, uint timeToLive, int retryCount = 0)
        {
            return this.AddIndexes(columnFamily, indexes, timeToLive);
        }

        // End Depricated Methods
    }
}