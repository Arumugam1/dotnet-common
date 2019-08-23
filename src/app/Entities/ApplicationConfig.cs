using System;
using System.Collections.Generic;
using System.Linq;
using automation.core.components.data.v1.Cassandra;
using automation.core.components.data.v1.Config;
using automation.core.components.data.v1.Entities;
using automation.core.components.operations.v1;

namespace automation.core.components.data.v1.Config
{
    public class ApplicationConfig
    {
        /// <summary>
        /// Unique Identifier
        /// </summary>
        public Guid ID { get; set; }

        /// <summary>
        /// Namespace for the configuration
        /// </summary>
        public string Namespace { get; set; }

        /// <summary>
        /// Name for configuraion 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Value to be stored
        /// </summary>
        public string Value { get; set; }

        /// <summary>
        /// Description about the config
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Environment(DEV/STAGE/PRODUCTION)
        /// </summary>
        public string Environment { get; set; }

        /// <summary>
        /// Time to CacheTime in application, so that no of get calls to data store is less on config that doesn't change often
        /// </summary>
        public TimeSpan CacheTime { get; set; }
    }

    public class SoftError
    {
        public Guid Id { get; set; }
        public string Error { get; set; }
        public DateTime AddedAt { get; set; }
        public string AddedBy { get; set; }

    }

    internal class ApplicationConfigBuffer
    {
        /// <summary>
        /// Application Config
        /// </summary>
        public ApplicationConfig ApplicationConfig { get; set; }

        /// <summary>
        /// Application Config is cache until this datetime is reached
        /// </summary>
        public DateTime CacheUntil { get; set; }
    }
}

namespace automation.core.components.data.v1.Providers
{
    public interface IApplicationConfigProvider
    {
        bool SetApplicationConfig(ApplicationConfig config);
        ApplicationConfig GetApplicationConfig(string name, string _namespace, string environment);
        ApplicationConfig GetApplicationConfigByName(string name);
    }
}

namespace automation.core.components.data.v1.Providers.Cassandra
{
    public sealed class ApplicationConfigProvider : DataExplorer<ApplicationConfig>, IApplicationConfigProvider
    {
        private const string Keyspace = "applicationconfig";
        private const string ColumnFamily = "applicationconfig";
        private const string KeyPrefix = "";

        private readonly IDataExplorer<ApplicationConfig> _dataExplorer;

        public ApplicationConfigProvider()
            : base(Keyspace, ColumnFamily, KeyPrefix)
        {
            _dataExplorer = this;
        }

        public ApplicationConfigProvider(IDataExplorer<ApplicationConfig> dataExplorer)
            : base(Keyspace, ColumnFamily, KeyPrefix)
        {
            _dataExplorer = dataExplorer;
        }

        bool Save(ApplicationConfig config)
        {
            return _dataExplorer.Add(string.Format("{0}.{1}.{2}", config.Environment, config.Namespace, config.Name), config);
        }

        bool IApplicationConfigProvider.SetApplicationConfig(ApplicationConfig config)
        {
            return Save(config);
        }

        ApplicationConfig IApplicationConfigProvider.GetApplicationConfig(string name, string _namespace, string environment)
        {
            return _dataExplorer.Get(string.Format("{0}.{1}.{2}", environment, _namespace, name));
        }

        ApplicationConfig IApplicationConfigProvider.GetApplicationConfigByName(string name)
        {
            return _dataExplorer.Get(string.Format("{0}.{1}.{2}", "", "", name));
        }
    }
}
