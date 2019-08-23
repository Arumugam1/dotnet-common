using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using StackExchange.Redis;
using automation.components.data.v1.Config;
using automation.components.data.v1.CustomTypes;
using automation.components.data.v1.Graph;
using automation.components.data.v1.Graph.Providers;
using automation.components.data.v1.Entities;
using automation.components.operations.v1;

namespace automation.components.data.v1.Redis
{
    /// <summary>
    /// Redis Sentinel Provider
    /// </summary>
    /// <typeparam name="T">dynamic entity</typeparam>
    public class StackExchangeRedisProvider : IRedisDataExplorer, IDisposable
    {
        private const int DefaultLimit = 1000;

        private static int retryLimit = Parser.ToInt(Manager.GetApplicationConfigValue("ConnectionRetryAttempts", "AllApplications.Redis"), 6);
        private static int retrySleep = Parser.ToInt(Manager.GetApplicationConfigValue("ConnectionRetryBackoff", "AllApplications.Redis"), 2000); // in ms
        private static Lazy<ConnectionMultiplexer> sentinelConnection =
            new Lazy<ConnectionMultiplexer>(() =>
            {
                // Expects a return of semicolon delemeted server:port;server:port
                var servers = Manager.GetApplicationConfigValue("EndPoints", "AllApplications.Redis.Sentinel");
                var serverStrings = servers.Split(';');

                var configuration = new ConfigurationOptions
                {
                    AbortOnConnectFail = false,
                    CommandMap = CommandMap.Sentinel,
                    TieBreaker = string.Empty,
                    ConnectTimeout = Parser.ToInt(Manager.GetApplicationConfigValue("ConnectionTimeOut", "AllApplications.Redis"), 5000),
                    SyncTimeout = Parser.ToInt(Manager.GetApplicationConfigValue("SyncTimeOut", "AllApplications.Redis"), 30000)
                };

                foreach (var serverString in serverStrings)
                {
                    configuration.EndPoints.Add(serverString);
                }

                return ConnectionMultiplexer.Connect(configuration);
            });

        private static Lazy<ConnectionMultiplexer> masterRedisConnection =
            new Lazy<ConnectionMultiplexer>(() =>
            {
                // Our Sentinel Redis service name
                var sentinelServiceName = Manager.GetApplicationConfigValue("ServiceName", "AllApplications.Redis.Sentinel");

                var configuration = new ConfigurationOptions
                {
                    AbortOnConnectFail = false,
                    ServiceName = sentinelServiceName,
                    TieBreaker = string.Empty,
                    ConnectTimeout = Parser.ToInt(Manager.GetApplicationConfigValue("ConnectionTimeOut", "AllApplications.Redis"), 5000),
                    SyncTimeout = Parser.ToInt(Manager.GetApplicationConfigValue("SyncTimeOut", "AllApplications.Redis"), 30000)
                };

                return sentinelConnection.Value.GetSentinelMasterConnection(configuration);
            });

        private static Lazy<ConfigurationOptions> redisConfigOptions =
            new Lazy<ConfigurationOptions>(() =>
            {
                var server = Manager.GetApplicationConfigValue("EndPoint", "AllApplications.Redis");
                string endpoint = string.Format("{0}:{1}", server, Parser.ToInt(Manager.GetApplicationConfigValue("Port", "AllApplications.Redis"), 0));
                var redisConnection = new ConfigurationOptions
                {
                    AbortOnConnectFail = false,
                    CommandMap = CommandMap.Twemproxy,
                    EndPoints =
                    {
                        { endpoint }
                    },
                    ConnectTimeout = Parser.ToInt(Manager.GetApplicationConfigValue("ConnectionTimeOut", "AllApplications.Redis"), 5000),
                    SyncTimeout = Parser.ToInt(Manager.GetApplicationConfigValue("SyncTimeOut", "AllApplications.Redis"), 30000)
                };
                return redisConnection;
            });

        private static Lazy<ConnectionMultiplexer> redisConnection =
            new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(redisConfigOptions.Value));

        private IGraphProvider graphProvider;

        public StackExchangeRedisProvider()
        {
            this.graphProvider = Container.Resolve<IGraphProvider>();
        }

        public static ConnectionMultiplexer Connection
        {
            get
            {
                return Parser.ToBoolean(Manager.GetApplicationConfigValue("Sentinel.Disabled", "AllApplications.Redis"), false) ? redisConnection.Value : masterRedisConnection.Value;
            }
        }

        public virtual bool Add(string key, string value)
        {
            return this.Add(key, value, 0);
        }

        public virtual bool Add(string key, string value, uint timeToLive)
        {
            var status = RetryStatus.Failed;
            this.WithRetryWithStatus<bool>(
                () =>
                {
                    var redis = this.RedisDatabase();
                    if (timeToLive == 0)
                    {
                        return redis.StringSet(key, value);
                    }
                    else
                    {
                        TimeSpan timeSpan = new TimeSpan(0, 0, Parser.ToInt(timeToLive, 0));
                        return redis.StringSet(key, value, timeSpan);
                    }
                },
                "Add",
                new object[] { key, value, timeToLive },
                out status);
            return status == RetryStatus.Success;
        }

        public virtual bool ConnectivityCheck()
        {
            return Connection.IsConnected;
        }

        public virtual bool Add<T>(string key, T value)
        {
            return this.Add(key, this.ValidateValue(JSon.Serialize(value)));
        }

        public bool Add<T>(string key, T value, uint timeToLive)
        {
            return this.Add(key, this.ValidateValue(JSon.Serialize(value)), timeToLive);
        }

        public virtual bool Add<T>(List<RedisEntity<T>> data)
        {
            var st = DateTime.UtcNow;
            data.RemoveAll(x => x == null);
            var kvs = new KeyValuePair<RedisKey, RedisValue>[data.Count];
            var count = 0;
            foreach (var datum in data)
            {
                kvs[count++] = new KeyValuePair<RedisKey, RedisValue>(datum.Key, this.ValidateValue(JSon.Serialize(datum.Value)));
            }

            var status = RetryStatus.Failed;
            var retVal = this.WithRetryWithStatus<bool>(() => this.RedisDatabase().StringSet(kvs), "Add", new object[] { data }, out status);
            this.ReportSlowQueries("Add", string.Join(",", data.Select(x => x.Key)), st);

            // Record TimeTaken
            if (this.graphProvider != null)
            {
                this.graphProvider.RecordTimeTaken(new Point(
                    Constants.RedLatency_Add,
                    (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }

            return status == RetryStatus.Success;
        }

        public virtual bool Add<T>(List<RedisEntity<T>> data, uint timeToLive)
        {
            if (timeToLive == 0)
            {
                return this.Add(data);
            }

            var st = DateTime.UtcNow;
            data.RemoveAll(x => x == null);
            var retVal = true;
            foreach (var datum in data)
            {
                retVal = retVal && this.Add(datum.Key, this.ValidateValue(JSon.Serialize(datum.Value)), timeToLive);
            }

            this.ReportSlowQueries("Add", string.Join(",", data.Select(x => x.Key)), st);

            // Record TimeTaken
            if (this.graphProvider != null)
            {
                this.graphProvider.RecordTimeTaken(new Point(
                    Constants.RedLatency_Add,
                    (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }

            return retVal;
        }

        public virtual T Get<T>(string key)
        {
            return this.Get<T>(new List<string> { key }, CommandFlags.None).FirstOrDefault();
        }

        public virtual string GetValue(string key)
        {
            return this.GetValue(key, CommandFlags.None);
        }

        public virtual string GetValue(string key, CommandFlags flag)
        {
            return this.WithRetry<string>(() => this.RedisDatabase().StringGet(key, flag), "GetValue", new object[] { key });
        }

        public virtual List<T> Get<T>(List<string> keys)
        {
            return this.Get<T>(keys, CommandFlags.None);
        }

        public virtual List<T> Get<T>(List<string> keys, CommandFlags flag)
        {
            if (keys == null || !keys.Any())
            {
                return new List<T>();
            }

            var st = DateTime.UtcNow;
            keys.RemoveAll(string.IsNullOrEmpty);

            var keyArray = new RedisKey[keys.Count];
            var count = 0;
            foreach (var key in keys)
            {
                keyArray[count++] = key;
            }

            var results = this.WithRetry<RedisValue[]>(() => this.RedisDatabase().StringGet(keyArray, flag), "Get", new object[] { keys }) ?? new RedisValue[0];
            var cols = new List<T>(results.Length);
            cols.AddRange(results.Select(result => JSon.Deserialize<T>(result)));

            // Record TimeTaken
            if (this.graphProvider != null)
            {
                this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_Get, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }

            return cols;
        }

        public virtual string GetAsString(string key)
        {
            return this.GetAsString(new List<string> { key }, CommandFlags.None).Values.FirstOrDefault();
        }

        public virtual Dictionary<string, string> GetAsString(List<string> keys)
        {
            return this.GetAsString(keys, CommandFlags.None);
        }

        public virtual Dictionary<string, string> GetAsString(List<string> keys, CommandFlags flag)
        {
            if (keys == null || !keys.Any())
            {
                return new Dictionary<string, string>();
            }

            var st = DateTime.UtcNow;
            keys.RemoveAll(x => string.IsNullOrEmpty(x));
            var uniqueKeys = keys.Distinct().ToList();
            var colVals = new Dictionary<string, string>();

            var keyArray = new RedisKey[uniqueKeys.Count];
            var count = 0;
            foreach (var key in uniqueKeys)
            {
                keyArray[count] = key;
                count++;
            }

            var results = this.WithRetry<RedisValue[]>(() => this.RedisDatabase().StringGet(keyArray, flag), "GetAsString", new object[] { keys }) ?? new RedisValue[0];
            if (results.Length > 0)
            {
                colVals = uniqueKeys.Zip(results, (k, v) => new { k, v }).ToDictionary(x => x.k, x => x.v.ToString());
            }

            // Record TimeTaken
            if (this.graphProvider != null)
            {
                this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_GetAsString, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }

            return colVals;
        }

        public virtual bool Delete(string key)
        {
            return this.WithRetry<bool>(
                () =>
                {
                    var st = DateTime.UtcNow;
                    key = this.ValidateName("key", key);
                    if (!this.RedisDatabase().KeyDelete(key))
                    {
                        return false;
                    }

                    if (this.graphProvider != null)
                    {
                        this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_Delete, (int)(DateTime.UtcNow - st).TotalMilliseconds));
                    }

                    return true;
                },
            "Delete",
            new object[] { key });
        }

        public virtual bool AddIndex(string key, string indexKey, string indexValue)
        {
            return this.AddIndexes(
                new List<Index>
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

        public virtual bool AddIndex(string key, string indexKey, string indexValue, uint timeToLive)
        {
            return this.AddIndexes(
                new List<Index>
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

        public virtual bool AddIndexes(Index index)
        {
            return this.AddIndexes(new List<Index> { index }, 0);
        }

        public virtual bool AddIndexes(Index index, uint timeToLive)
        {
            return this.AddIndexes(new List<Index> { index }, timeToLive);
        }

        public virtual bool AddIndexes(List<Index> indexes)
        {
            return this.AddIndexes(indexes, 0);
        }

        public virtual bool AddIndexes(List<Index> indexes, uint timeToLive)
        {
            var st = DateTime.UtcNow;
            indexes.RemoveAll(ind => string.IsNullOrEmpty(ind.Name));
            var retVal = true;
            var groupedIndexes = indexes.GroupBy(index => index.Name, index => index.KeyValues);
            foreach (var grouped in groupedIndexes)
            {
                var hashSet = new HashSet<HashEntry>(new HashEntryEqualityOperator());
                foreach (var dict in grouped)
                {
                    foreach (var kv in dict)
                    {
                        hashSet.Add(new HashEntry(kv.Key, kv.Value));
                    }
                }

                var status = RetryStatus.Failed;
                this.WithRetryWithStatus<bool>(
                    () =>
                    {
                        this.RedisDatabase().HashSet(grouped.Key, hashSet.ToArray());
                        return true;
                    },
                    "AddIndexes",
                    new object[] { grouped.Key, hashSet },
                    out status);
                retVal = retVal && status == RetryStatus.Success;
            }

            // Record TimeTaken
            if (this.graphProvider != null)
            {
                this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_AddIndexes, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }

            return retVal;
        }

        public virtual Dictionary<string, string> GetIndex(string key)
        {
            return this.GetIndex(key, DefaultLimit, CommandFlags.None);
        }

        public virtual Dictionary<string, string> GetIndex(string key, int limit)
        {
            return this.GetIndex(key, limit, CommandFlags.None);
        }

        public virtual Dictionary<string, string> GetIndex(string key, int limit, CommandFlags flag)
        {
            var st = DateTime.UtcNow;
            key = this.ValidateName("key", key);
            var hashEntries = this.WithRetry<HashEntry[]>(() => this.RedisDatabase().HashGetAll(key, flag), "GetIndex", new object[] { key });

            // Record TimeTaken
            if (this.graphProvider != null)
            {
                this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_GetIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }

            if (hashEntries.Length == 0)
            {
                return null;
            }
            else
            {
                return hashEntries.ToDictionary<HashEntry, string, string>(entry => entry.Name, entry => entry.Value);
            }
        }

        public virtual List<string> GetIndexColumnNames(string key)
        {
            return this.GetIndexColumnNames(key, DefaultLimit, CommandFlags.None);
        }

        public virtual List<string> GetIndexColumnNames(string key, int limit)
        {
            return this.GetIndexColumnNames(key, limit, CommandFlags.None);
        }

        public virtual List<string> GetIndexColumnNames(string key, int limit, CommandFlags flag)
        {
            var st = DateTime.UtcNow;

            key = this.ValidateName("key", key);

            var hashEntries = this.WithRetry<HashEntry[]>(() => this.RedisDatabase().HashGetAll(key, flag), "GetIndexColumnNames", new object[] { key }) ?? new HashEntry[0];

            // Record TimeTaken
            if (this.graphProvider != null)
            {
                this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_GetIndexColumnNames, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }

            if (hashEntries.Length == 0)
            {
                return null;
            }
            else
            {
                return hashEntries.Select(entry => (string)entry.Name).ToList();
            }
        }

        public virtual List<string> GetIndexColumnValues(string key)
        {
            return this.GetIndexColumnValues(key, DefaultLimit, CommandFlags.None);
        }

        public virtual List<string> GetIndexColumnValues(string key, int limit)
        {
            return this.GetIndexColumnValues(key, limit, CommandFlags.None);
        }

        public virtual List<string> GetIndexColumnValues(string key, int limit, CommandFlags flag)
        {
            var st = DateTime.UtcNow;

            key = this.ValidateName("key", key);
            var hashEntries = this.WithRetry<HashEntry[]>(() => this.RedisDatabase().HashGetAll(key, flag), "GetIndexColumnValues", new object[] { key });

            // Record TimeTaken
            if (this.graphProvider != null)
            {
                this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_GetIndexColumnValues, (int)(DateTime.UtcNow - st).TotalMilliseconds));
            }

            if (hashEntries == null)
            {
                return null;
            }
            else
            {
                return hashEntries.Select(entry => (string)entry.Value).ToList();
            }
        }

        public virtual string GetIndexValue(string key, string indexKey)
        {
            return this.GetIndexValue(key, indexKey, CommandFlags.None);
        }

        public virtual string GetIndexValue(string key, string indexKey, CommandFlags flag)
        {
            return this.WithRetry<RedisValue>(
                () =>
                {
                    var st = DateTime.UtcNow;

                    key = this.ValidateName("key", key);
                    var returnValue = this.RedisDatabase().HashGet(key, indexKey, flag);
                    if (this.graphProvider != null)
                    {
                        this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_GetIndexValue, (int)(DateTime.UtcNow - st).TotalMilliseconds));
                    }

                    return returnValue;
                },
            "GetIndexValue",
            new object[] { key, indexKey });
        }

        public virtual bool DeleteIndex(string key)
        {
            return this.WithRetry<bool>(
                () =>
                {
                    var st = DateTime.UtcNow;
                    key = this.ValidateName("key", key);

                    if (!this.RedisDatabase().KeyDelete(key))
                    {
                        return false;
                    }

                    // Record TimeTaken
                    if (this.graphProvider != null)
                    {
                        this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_DeleteIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));
                    }

                    return true;
                },
                "DeleteIndex",
                new object[] { key });
        }

        public virtual bool DeleteIndex(string key, string indexKey)
        {
            return this.DeleteIndex("column1", key, indexKey);
        }

        public virtual bool DeleteIndex(string columnName, string key, string indexKey)
        {
            return this.WithRetry<bool>(
                () =>
                {
                    var st = DateTime.UtcNow;
                    key = this.ValidateName("key", key);

                    if (!this.RedisDatabase().HashDelete(key, indexKey))
                    {
                        return false;
                    }

                    // Record TimeTaken
                    if (this.graphProvider != null)
                    {
                        this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_DeleteIndex, (int)(DateTime.UtcNow - st).TotalMilliseconds));
                    }

                    return true;
                },
            "DeleteIndex",
            new object[] { columnName, key, indexKey });
        }

        public virtual int Count(string key)
        {
            return this.Count(key, DefaultLimit, CommandFlags.None);
        }

        public virtual int Count(string key, int limit)
        {
            return this.Count(key, limit, CommandFlags.None);
        }

        public virtual int Count(string key, int limit, CommandFlags flag)
        {
            return this.WithRetry<int>(
                () =>
                {
                    var st = DateTime.UtcNow;
                    key = this.ValidateName("key", key);

                    int count = (int)this.RedisDatabase().HashLength(key, flag);

                    // Record TimeTaken
                    if (this.graphProvider != null)
                    {
                        this.graphProvider.RecordTimeTaken(new Point(Constants.RedLatency_Count, (int)(DateTime.UtcNow - st).TotalMilliseconds));
                    }

                    return count;
                },
            "Count",
            new object[] { key });
        }

        public virtual Guid GetTimeBasedGuid(DateTime dt)
        {
            return CryptographicTool.GetTimeBasedGuid(dt);
        }

        public virtual IDatabase RedisDatabase()
        {
            return Connection.GetDatabase();
        }

        public virtual void Dispose()
        {
        }

        private void ExceptionHandler(Exception ex, string methodName, object[] paramsObjects)
        {
            if (ex is RedisConnectionException)
            {
                this.HandleRedisConnectionException((RedisConnectionException)ex);
            }
            else
            {
                this.HandleException();
            }

            Trace.Exception(
                TraceTypes.AccessLayer,
                string.Format("{0}.{1}", this.GetType().Namespace ?? string.Empty, this.GetType().Name),
                methodName,
                paramsObjects,
                ex);
        }

        private void HandleRedisConnectionException(RedisConnectionException rcx)
        {
            if (rcx.FailureType == ConnectionFailureType.UnableToConnect || rcx.FailureType == ConnectionFailureType.UnableToResolvePhysicalConnection)
            {
                this.RecordException(Constants.RedisConnectionFailed);
            }
            else
            {
                this.RecordException(Constants.RedisConnectionFailedOtherType);
            }
        }

        private void HandleException()
        {
            this.RecordException(Constants.RedisException);
        }

        private void RecordException(string point)
        {
            if (this.graphProvider != null)
            {
                this.graphProvider.Record(new Point(point));
            }
        }

        private string ValidateName(string key, string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                throw new NullReferenceException(key);
            }

            return value;
        }

        private string ValidateValue(string value)
        {
            return value ?? "NULL";
        }

        private void ReportSlowQueries(string methodName, string query, DateTime requestTime)
        {
            this.ReportSlowQueries(methodName, query, requestTime, DateTime.UtcNow);
        }

        private async Task<bool> AddAsync(string key, string value, uint timeToLive)
        {
            return await this.WithRetryAsync<bool>(
                () =>
                {
                    return this.RedisDatabase().StringSetAsync(key, value, new TimeSpan(0, 0, (int)timeToLive));
                },
                "AddAsync",
                new object[] { key, value, timeToLive }).ConfigureAwait(false);
        }

        private async Task<bool> HashSetAsync(RedisKey key, RedisValue hashValue, RedisValue value)
        {
            return await this.WithRetryAsync<bool>(
                () =>
                {
                    return this.RedisDatabase().HashSetAsync(key, hashValue, value);
                },
                "HashSetAsync",
                new object[] { key, hashValue, value }).ConfigureAwait(false);
        }

        private TResult WithRetryWithStatus<TResult>(Func<TResult> action, string methodName, object[] paramObjects, out RetryStatus status)
        {
            return Retry.Execute<TResult>(action, retrySleep, retryLimit, typeof(RedisConnectionException), this.ExceptionHandler, methodName, paramObjects, out status);
        }

        private TResult WithRetry<TResult>(Func<TResult> action, string methodName, object[] paramObjects)
        {
            var status = RetryStatus.Failed;
            return Retry.Execute<TResult>(action, retrySleep, retryLimit, typeof(RedisConnectionException), this.ExceptionHandler, methodName, paramObjects, out status);
        }

        private async Task<TResult> WithRetryAsync<TResult>(Func<Task<TResult>> action, string methodName, object[] paramObjects)
        {
            return await Retry.ExecuteAsync<TResult>(action, retrySleep, retryLimit, typeof(RedisConnectionException), this.ExceptionHandler, methodName, paramObjects);
        }

        private void ReportSlowQueries(string methodName, string query, DateTime requestTime, DateTime responseTime)
        {
            var tT = responseTime - requestTime;

            if (tT.TotalSeconds < 2)
            {
                return;
            }

            try
            {
                var traceObj = new object[]
                    {
                        "Possible slow query detected",
                        new Dictionary<string, dynamic>
                        {
                            { "Query", query },
                            { "Time Taken", tT },
                            { "Request Time", requestTime },
                            { "Response Time", responseTime }
                        }
                    };
                Trace.Warning(
                    TraceTypes.AccessLayer,
                    "automation.components.data.v1.Redis.Communicator<T>",
                    methodName,
                    traceObj);
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v1.Redis.DataExplorer",
                    "ReportSlowQueries",
                    new object[] { query },
                    ex);
            }
        }

        public class HashEntryEqualityOperator : IEqualityComparer<HashEntry>
        {
            public bool Equals(HashEntry x, HashEntry y)
            {
                return x.Name == y.Name;
            }

            public int GetHashCode(HashEntry obj)
            {
                return obj.Name.GetHashCode();
            }
        }
    }
}
