using System;
using System.Configuration;
using System.Diagnostics;
using automation.components.data.v1.Graph;
using automation.components.data.v1.Graph.Providers;
using automation.components.data.v1.Graph.Providers.Graphite;
using automation.components.operations.v1;
using Dse;

namespace automation.components.data.v1.Cassandra
{
    public static class DSEConnection
    {
        private static bool IsCassandraTraceEnabled
        {
            get
            {
                return Parser.ToBoolean(ConfigurationManager.AppSettings["CassandraTraceEnabled"], false);
            }
        }

        static Cluster cluster;
        //Executed once per app domain
        static readonly ISession _session = Connect();

        public static ISession Session
        {
            get
            {
                return _session;
            }
        }

        static ISession Connect()
        {
            var connectionString = ConfigurationManager.AppSettings["cassandraServerConfig"];
            var connectionStringParser = new DSEConnectionStringParser(new DSEConnectionBuilderWrapper());

            var builder = connectionStringParser.Parse(connectionString);

            if (IsCassandraTraceEnabled)
            {
                Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            }

            cluster = builder.Build();
            RecordMetrics("Open");
            return cluster.Connect();
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
                automation.components.operations.v1.Trace.Exception(TraceTypes.AccessLayer, "DSEConnection", "RecordMetrics", new object[] { }, ex);
            }
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
