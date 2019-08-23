using automation.components.data.v1.Cassandra;
using automation.components.data.v1.Config;

namespace automation.components.data.v1.Providers.Cassandra
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
            return _dataExplorer.Add(config.Key(), config);
        }

        bool IApplicationConfigProvider.SetApplicationConfig(ApplicationConfig config)
        {
            return Save(config);
        }

        ApplicationConfig IApplicationConfigProvider.GetApplicationConfig(string name, string _namespace, string environment)
        {
            return _dataExplorer.Get(ApplicationConfig.Key(environment, _namespace, name));
        }

        ApplicationConfig IApplicationConfigProvider.GetApplicationConfigByName(string name)
        {
            return _dataExplorer.Get(ApplicationConfig.Key("", "", name));
        }
    }
}
