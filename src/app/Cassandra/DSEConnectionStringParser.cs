namespace automation.components.data.v1.Cassandra
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using automation.components.operations.v1;
    using System.Security.Authentication;
    using Dse;

    /// <summary>
    /// This class is responsible for parsing the cassandra connection string into a properly configured
    /// ClusterBuilder object.
    /// Connection string is a string in the form: "Key1 = V1, V2; Key2 = V3; ....".  The supported keys
    /// and their defaults can be seen in the 'defaultConfig' private member variable.
    /// </summary>
    public class DSEConnectionStringParser
    {
        private readonly IDSEConnectionBuilder clusterBuilder;

        private readonly ConfigItem[] defaultConfig = new ConfigItem[]
        {
            new ConfigItem("MaxProtocolVersion", "3", (x) => byte.Parse(x)),
            new ConfigItem("DataCenter", "DC1", (x) => x),
            new ConfigItem("WithLoadbalancing", "true", (x) => bool.Parse(x)),
            new ConfigItem("SSL", "true", (x) => bool.Parse(x)),
            new ConfigItem("Connection Timeout", "10000", (x) => int.Parse(x)),
            new ConfigItem("Max Concurrency", "128", (x) => int.Parse(x)),
            new ConfigItem("Consistency Level", "1", (x) => (ConsistencyLevel)int.Parse(x)),
            new ConfigItem("HeartbeatInterval", "50000", (x) => int.Parse(x)),
            new ConfigItem("MaxRetry", "3", (x) => int.Parse(x)),
            new ConfigItem("Backoff", "2000", (x) => int.Parse(x)),
            new ConfigItem("UserName", null, (x) => x),
            new ConfigItem("Password", null, (x) => x),
            new ConfigItem("Server", null, (x) =>
            {
                if (x == null)
                {
                    throw new ArgumentException("You must supply one or more hosts with 'Server = x,y,z'");
                }
                return x.Split(',').Select(y => y.Trim()).ToArray();
            })
        };

        private Dictionary<string, object> normalizedConfig;

        /// <summary>
        /// Initializes a new instance of the <see cref="DSEConnectionStringParser"/> class.
        /// Takes in a cluster builder object to configure.  The actual cluster builder object in
        /// the Datastax client was wrapped to support an interface (IDSEConnectionBuilder) for mocking in tests.
        /// </summary>
        /// <param name="clusterBuilder">automation.components.data.v1.Cassandra.BuilderWrapper implements IDSEConnectionBuilder</param>
        public DSEConnectionStringParser(IDSEConnectionBuilder clusterBuilder)
        {
            this.clusterBuilder = clusterBuilder;
        }

        /// <summary>
        /// Method that Parses the connection string and configures a cluster builder with the proper connection options.
        /// </summary>
        /// <param name="connectionString">Cassandra connection string.  Must contain a "Server = x" param</param>
        /// <returns>Properly configured cluster builder</returns>
        public IDSEConnectionBuilder Parse(string connectionString)
        {
            var rawValues = this.ToRawValues(connectionString);

            this.normalizedConfig = this.NormalizeConfig(rawValues);

            this.LogSanitizedConfig();

            this.ConfigureClusterBuilder();

            return this.clusterBuilder;
        }

        private void ConfigureClusterBuilder()
        {
            this.clusterBuilder
                .AddContactPoints((string[])this.normalizedConfig["Server"])
                .WithQueryTimeout((int)this.normalizedConfig["Connection Timeout"])
                .WithReconnectionPolicy(new ConstantReconnectionPolicy(100L))
                .WithMaxProtocolVersion((byte)this.normalizedConfig["MaxProtocolVersion"]);

            this.AddSSLOptions();
            this.AddPoolingOptions();
            this.AddRetryPolicy();
            this.AddLBPolicy();
            this.AddQueryOptions();
            this.AddCredentials();
        }

        private void LogSanitizedConfig()
        {
            var sanitizedConfig = this.normalizedConfig.Where(kv => kv.Key != "Password");
            System.Diagnostics.Trace.WriteLine(JSon.Serialize(sanitizedConfig));
        }

        private void AddSSLOptions()
        {
            if ((bool)this.normalizedConfig["SSL"])
            {
                var sslOptions = new SSLOptions(SslProtocols.Tls12, false, null);
                this.clusterBuilder.WithSSL(sslOptions);
            }
        }

        private void AddCredentials()
        {
            string user = (string)this.normalizedConfig["UserName"];
            string password = (string)this.normalizedConfig["Password"];

            if (user != null && password != null)
            {
                this.clusterBuilder.WithCredentials(user, password);
            }
        }

        private void AddLBPolicy()
        {
            if ((bool)this.normalizedConfig["WithLoadbalancing"])
            {
                var lbPolicy = new TokenAwarePolicy(new DCAwareRoundRobinPolicy((string)this.normalizedConfig["DataCenter"]));
                this.clusterBuilder.WithLoadBalancingPolicy(lbPolicy);
            }
        }

        private void AddRetryPolicy()
        {
            int maxRetry = (int)this.normalizedConfig["MaxRetry"];
            int backoff = (int)this.normalizedConfig["Backoff"];
            this.clusterBuilder.WithRetryPolicy(new DSERBARetryPolicy(maxRetry, maxRetry, maxRetry, backoff));
        }

        private void AddPoolingOptions()
        {
            PoolingOptions poolingOptions = new PoolingOptions();
            var interval = (int)this.normalizedConfig["HeartbeatInterval"];
            var concurrency = (int)this.normalizedConfig["Max Concurrency"];

            poolingOptions.SetHeartBeatInterval(interval);
            poolingOptions.SetMaxSimultaneousRequestsPerConnectionTreshold(HostDistance.Local, concurrency);
            this.clusterBuilder.WithPoolingOptions(poolingOptions);
        }

        private void AddQueryOptions()
        {
            QueryOptions queryOptions = new QueryOptions();
            queryOptions.SetConsistencyLevel((ConsistencyLevel)this.normalizedConfig["Consistency Level"]);
            this.clusterBuilder.WithQueryOptions(queryOptions);
        }

        private Dictionary<string, object> NormalizeConfig(Dictionary<string, string> rawValues)
        {
            return this.defaultConfig.Select(cItem =>
            {
                var value = rawValues.ContainsKey(cItem.Key) ? rawValues[cItem.Key] : cItem.DefaultValue;
                return new[] { cItem.Key, cItem.ValueConverter(value) };
            }).ToDictionary(t => (string)t[0], t => t[1]);
        }

        private Dictionary<string, string> ToRawValues(string connectionString)
        {
            return connectionString.Split(';')
                .Where(kvp => kvp.Contains('='))
                .Select(kvp => kvp.Split(new[] { '=' }, 2))
                .ToDictionary(kvp => kvp[0].Trim(), kvp => kvp[1].Trim(), StringComparer.InvariantCultureIgnoreCase);
        }

        private class ConfigItem
        {
            public ConfigItem(string key, string defaultValue, Func<string, object> valueConverter)
            {
                this.Key = key;
                this.DefaultValue = defaultValue;
                this.ValueConverter = valueConverter;
            }

            public string Key { get; set; }

            public string DefaultValue { get; set; }

            public Func<string, object> ValueConverter { get; set; }
        }
    }
}
