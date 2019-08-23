using Graphite;
using Graphite.StatsD;
using System;
using System.Collections.Generic;
using automation.components.operations.v1;
using System.Configuration;
using System.Threading;

namespace automation.components.data.v1.Graph
{
    public class Point
    {
        public string Axis { get; set; }
        public int Value { get; set; }

        public Point(string Axis, int Value)
        {
            this.Axis = Axis.ToLower();
            this.Value = Value;
        }

        public Point(string Axis)
        {
            this.Axis = Axis.ToLower();
            this.Value = 1;
        }
    }
}

namespace automation.components.data.v1.Graph.Providers
{
    public interface IGraphProvider
    {
        void Record(Point graph);
        void RecordTimeTaken(Point graph);
        void RecordAt(Point graph);
        void RecordAt(Point graph, DateTime OcurredAt);
        void DeRecord(Point point);
    }
}

namespace automation.components.data.v1.Graph.Providers.Graphite
{
    public sealed class GraphProvider : IGraphProvider
    {
        private readonly GraphiteTcpClient graphiteClient;
        private readonly StatsDClient statsDClient;
        private readonly double statsDSampleRate = 1;

        public GraphProvider()
        {
            //Retrieve data from app config
            string graphServerConfig = ConfigurationManager.AppSettings["graphServerConfig"];
            try
            {
                if (!string.IsNullOrEmpty(graphServerConfig))
                    if (graphServerConfig.Split(':').Length >= 4)
                        if (graphServerConfig.Split(':')[0].ToLower().Trim() == "statsd")
                            statsDClient = new StatsDClient(graphServerConfig.Split(':')[1],
                                                            Parser.ToInt(graphServerConfig.Split(':')[2], 8125),
                                                            string.Format(graphServerConfig.Split(':')[3], FormatPath(Environment.MachineName)));
                        else if (graphServerConfig.Split(':')[0].ToLower().Trim() == "graphite")
                            graphiteClient = new GraphiteTcpClient(graphServerConfig.Split(':')[1],
                                                                   Parser.ToInt(graphServerConfig.Split(':')[2], 2003),
                                                                   string.Format(graphServerConfig.Split(':')[3], FormatPath(Environment.MachineName)));

                if (statsDClient == null && !string.IsNullOrEmpty(ConfigurationManager.AppSettings["statsDServerConfig"]))
                    switch (ConfigurationManager.AppSettings["statsDServerConfig"].Split(':').Length)
                    {
                        case 1:
                            statsDClient = new StatsDClient(ConfigurationManager.AppSettings["statsDServerConfig"].Split(':')[0],
                                                            8125,
                                                            FormatPath(Environment.MachineName));
                            break;
                        case 2:
                            statsDClient = new StatsDClient(ConfigurationManager.AppSettings["statsDServerConfig"].Split(':')[0],
                                                            Parser.ToInt(ConfigurationManager.AppSettings["statsDServerConfig"].Split(':')[1], 2003),
                                                            FormatPath(Environment.MachineName));
                            break;
                        case 3:
                            statsDClient = new StatsDClient(ConfigurationManager.AppSettings["statsDServerConfig"].Split(':')[0],
                                                            Parser.ToInt(ConfigurationManager.AppSettings["statsDServerConfig"].Split(':')[1], 2003),
                                                            string.Format(ConfigurationManager.AppSettings["statsDServerConfig"].Split(':')[2], FormatPath(Environment.MachineName)));
                            break;
                    }

                //Retrieve data from app config
                string graphiteServerConfig = ConfigurationManager.AppSettings["graphiteServerConfig"]; 
                if (graphiteClient == null && !string.IsNullOrEmpty(graphiteServerConfig))
                    switch (graphiteServerConfig.Split(':').Length)
                    {
                        case 1:
                            graphiteClient = new GraphiteTcpClient(graphiteServerConfig.Split(':')[0],
                                                                   2003,
                                                                   FormatPath(Environment.MachineName));
                            break;
                        case 2:
                            graphiteClient = new GraphiteTcpClient(graphiteServerConfig.Split(':')[0],
                                                                   Parser.ToInt(graphiteServerConfig.Split(':')[1], 2003),
                                                                   FormatPath(Environment.MachineName));
                            break;
                        case 3:
                            graphiteClient = new GraphiteTcpClient(graphiteServerConfig.Split(':')[0],
                                                                   Parser.ToInt(graphiteServerConfig.Split(':')[1], 2003),
                                                                   string.Format(graphiteServerConfig.Split(':')[2], FormatPath(Environment.MachineName)));
                            break;
                    }
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer,
                    "automation.components.data.v1.Graph.Providers.Graphite.GraphProvider",
                    "GraphProvider",
                    new object[] { graphServerConfig, statsDClient, graphiteClient}
                    , ex);
            }
        }

        public GraphProvider(GraphiteTcpClient graphiteClient)
        {
            this.graphiteClient = graphiteClient;
        }

        public GraphProvider(StatsDClient statsDClient)
        {
            this.statsDClient = statsDClient;
        }

        public GraphProvider(GraphiteTcpClient graphiteClient, StatsDClient statsDClient)
        {
            this.statsDClient = statsDClient;
            this.graphiteClient = graphiteClient;
        }

        string FormatPath(string path)
        {
            return path.Replace("-", "").Replace(" ", "").Replace(":", "_");
        }

        void IGraphProvider.Record(Point graph)
        {
            if (statsDClient != null)
                statsDClient.Increment(FormatPath(graph.Axis), graph.Value, statsDSampleRate);

            if (graphiteClient != null)
                graphiteClient.Send(FormatPath(graph.Axis), graph.Value);
        }

        void IGraphProvider.DeRecord(Point point)
        {
            if (statsDClient != null)
                statsDClient.Decrement(FormatPath(point.Axis), point.Value, statsDSampleRate);

            if (graphiteClient != null)
                graphiteClient.Send(FormatPath(point.Axis), point.Value);
        }

        void IGraphProvider.RecordTimeTaken(Point graph)
        {
            if (statsDClient != null)
                statsDClient.Timing(FormatPath(graph.Axis), graph.Value, statsDSampleRate);
        }

        void IGraphProvider.RecordAt(Point graph)
        {
            if (statsDClient != null)
                statsDClient.Increment(FormatPath(graph.Axis), graph.Value, statsDSampleRate);

            if (graphiteClient != null)
                graphiteClient.Send(FormatPath(graph.Axis), graph.Value);

            //Async Point every 10 sec
            //ProcessPoint(graph.Axis, graph.Value);
        }

        void IGraphProvider.RecordAt(Point graph, DateTime At)
        {
            if (statsDClient != null)
                statsDClient.Increment(FormatPath(graph.Axis), graph.Value, statsDSampleRate);

            if (graphiteClient != null)
                graphiteClient.Send(FormatPath(graph.Axis), graph.Value, At);
        }

        private object lstCollectPointLock = new object();
        private Dictionary<string, int> lstCollectPoint = new Dictionary<string, int>();
        private Thread flushThread;
        private void ProcessPoint(string Axis, int Value)
        {
            if (graphiteClient != null || statsDClient != null)
                lock (lstCollectPointLock)
                {
                    if (!lstCollectPoint.ContainsKey(Axis))
                        lstCollectPoint.Add(Axis, 0);

                    if (flushThread == null || flushThread.ThreadState != ThreadState.Running)
                    {
                        flushThread = new Thread(new ThreadStart(FlushEvery10Sec));
                        flushThread.Start();
                    }

                    lstCollectPoint[Axis] = lstCollectPoint[Axis] + Value;
                }
        }

        private void FlushEvery10Sec()
        {
            Thread.Sleep(10000);

            lock (lstCollectPointLock)
            {
                if (statsDClient != null)
                    foreach (var Key in lstCollectPoint.Keys)
                        statsDClient.Increment(FormatPath(Key), lstCollectPoint[Key], 10);

                if (graphiteClient != null)
                    foreach (var Key in lstCollectPoint.Keys)
                        graphiteClient.Send(FormatPath(Key), lstCollectPoint[Key], DateTime.Now);

                lstCollectPoint = new Dictionary<string, int>();
            }
        }
    }
}
