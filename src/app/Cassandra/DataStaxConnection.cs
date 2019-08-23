using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using automation.core.components.data.v1.Graph;
using automation.core.components.data.v1.Graph.Providers;
using automation.core.components.data.v1.Graph.Providers.Graphite;
using automation.core.components.operations.v1;
using Cassandra;

namespace automation.core.components.data.v1.Cassandra
{
    public static class DataStaxConnection
    {
        private static bool IsCassandraTraceEnabled
        {
            get
            {
                return Parser.ToBoolean(ConfigurationManager.AppSettings["CassandraTraceEnabled"], false);
            }
        }

        static Dictionary<string, string> appSettings = new Dictionary<string, string>();
        static Cluster cluster;
        //Executed once per app domain
        static readonly ISession _session = Connect();
        
        static DataStaxConnection()
        {
        }


        public static ISession Session
        {
            get
            {
                return _session;
            }
        }

        static ISession Connect()
        {
            ParseConnectionString();
            var builder = Cluster.Builder();

            if (IsCassandraTraceEnabled)
                Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;

            //System.Diagnostics.Trace.Listeners.Add(new ConsoleTraceListener());

            PoolingOptions options = new PoolingOptions();
            options.SetHeartBeatInterval(50000);
            options.SetCoreConnectionsPerHost(HostDistance.Local, options.GetCoreConnectionsPerHost(HostDistance.Local));
            int maxConcurrency = GetMaxConcurrency();
            options.SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, maxConcurrency);
            QueryOptions queryOptions = new QueryOptions();
            SetConsistencyLevel(queryOptions);

            //connection timeout 
            var timeout = GetConnectionTimeout();
            //data center name
            var localDC = GetDCName();

            // RetryPolicy: TODO: constructor values from app.config
            int maxRetry = 3;
            int backoff = 2000;
            RBARetryPolicy retryPolicy = new RBARetryPolicy(maxRetry, maxRetry, maxRetry, backoff);
            var maxProtocolVersion = GetMaxProtocolVersion();
            SSLOptions sslOptions = new SSLOptions(SslProtocols.Tls12, false, null);

            if (appSettings.ContainsKey("UserName") && !string.IsNullOrWhiteSpace(appSettings["UserName"]) &&
                appSettings.ContainsKey("Password") && !string.IsNullOrWhiteSpace(appSettings["Password"]))
            {
                cluster = builder
                    .WithPoolingOptions(options)
                    .AddContactPoints(appSettings["Server"].Split(','))
                    .WithQueryTimeout(timeout) // <- 10 sec client timeout
                    .WithRetryPolicy(retryPolicy)
                    .WithReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                    .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDC)))
                    .WithQueryOptions(queryOptions)
                    .WithCredentials(appSettings["UserName"], appSettings["Password"])
                    .WithMaxProtocolVersion(maxProtocolVersion)
                    .WithSSL(sslOptions)
                    .Build();
            }
            else
            {
                cluster = builder
                    .WithPoolingOptions(options)
                    .AddContactPoints(appSettings["Server"].Split(','))
                    .WithQueryTimeout(timeout) // <- 10 sec client timeout
                    .WithRetryPolicy(retryPolicy)
                    .WithReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                    .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDC)))
                    .WithQueryOptions(queryOptions)
                    .WithMaxProtocolVersion(maxProtocolVersion)
                    .WithSSL(sslOptions)
                    .Build();
            }
            RecordMetrics("Open");
            return cluster.Connect();
        }

        private static byte GetMaxProtocolVersion()
        {
            string maxProtocolVersion;
            var version = appSettings.TryGetValue("MaxProtocolVersion", out maxProtocolVersion) ? maxProtocolVersion : "3";

            byte byteVersion;
            return byte.TryParse(version, out byteVersion) ? byteVersion : (byte)3;
        }

        private static void RecordMetrics(string connectionStatus)
        {
            try
            {
                IGraphProvider graphProvider = new GraphProvider();
                graphProvider.Record(
                    new Point(string.Format("{0}.{1}", Constants.CassandraConnection, connectionStatus)));
            }
            catch (Exception ex)
            {
                automation.core.components.operations.v1.Trace.Exception(TraceTypes.AccessLayer, "DataStaxConnection", "RecordMetrics", new object[] { }, ex);
            }
        }

        private static string GetDCName()
        {
            string dc;
            var localDC = appSettings.TryGetValue("DataCenter", out dc) ? dc : "DC1";
            return localDC;
        }

        private static int GetConnectionTimeout()
        {
            string conTimeout;
            return appSettings.TryGetValue("Connection Timeout", out conTimeout) ? Parser.ToInt(conTimeout, 10000) : 10000;
        }

        private static int GetMaxConcurrency()
        {
            string concurrency;
            return appSettings.TryGetValue("Max Concurrency", out concurrency) ? Convert.ToInt32(concurrency) : 128;
        }

        private static void SetConsistencyLevel(QueryOptions queryOptions)
        {
            string consistencyValue;
            if (appSettings.TryGetValue("Consistency Level", out consistencyValue))
            {
                var consistencyLevel = Parser.ToInt(consistencyValue, 1);
                if (Enum.IsDefined(typeof(ConsistencyLevel), consistencyLevel))
                {
                    ConsistencyLevel conl = (ConsistencyLevel)Enum.Parse(typeof(ConsistencyLevel), consistencyValue);
                    queryOptions.SetConsistencyLevel(conl);
                }
                else
                    queryOptions.SetConsistencyLevel(ConsistencyLevel.One);
            }
            else
                queryOptions.SetConsistencyLevel(ConsistencyLevel.One);
        }

        private static void ParseConnectionString()
        {
            appSettings = ConfigurationManager.AppSettings["cassandraServerConfig"].Split(new char[] { ';' })
                .Where(kvp => kvp.Contains('='))
                .Select(kvp => kvp.Split(new char[] { '=' }, 2))
                .ToDictionary(kvp => kvp[0].Trim(), kvp => kvp[1].Trim(),
                    StringComparer.InvariantCultureIgnoreCase);
        }

        public static void Close()
        {
            if (cluster != null)
            {
                RecordMetrics("Close");
                cluster.Shutdown();
            }

            if (_session != null)
                Session.Dispose();
        }
    }
}
