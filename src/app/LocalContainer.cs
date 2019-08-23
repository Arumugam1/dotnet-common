using System.Data;
using System.Globalization;
using automation.core.components.data.v1.Config;
using automation.core.components.data.v1.Local.Cassandra;
using automation.core.components.data.v1.Providers;
using automation.core.components.data.v1.Providers.Cassandra;
using automation.core.components.operations.v1;
using automation.core.components.operations.v1.JSonExtensions.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Reflection;

namespace automation.core.components.data.v1
{
    public static class LocalContainer
    {

        public static bool Initialized { get; set; }

        private static object lckInit = new object();

        public static Dictionary<string, Dictionary<string, Dictionary<string, string>>> data =
            new Dictionary<string, Dictionary<string, Dictionary<string, string>>>();
        public static DataSet sqlData = new DataSet();
        public static List<string> graphiteSaveData = new List<string>();
        public static EmailMockClient emailMockClient = new EmailMockClient();
        public static Dictionary<string, string> redisData = new Dictionary<string, string>();
        public static Dictionary<string, Dictionary<string, string>> redisIndexData = new Dictionary<string, Dictionary<string, string>>();

        public static Dictionary<string, Dictionary<string, Dictionary<string, string>>> DB
        {
            get { return data; }
        }

        public static DataSet SQLDB
        {
            get { return sqlData; }
        }

        public static void Init()
        {
            if (!Initialized)
                lock (lckInit)
                    if (!Initialized)
                    {
                        var currentOperation = string.Empty;

                        InitData();

                        try
                        {
                            InitIniterfaces();

                            Initialized = true;
                        }
                        catch
                        {
                            Initialized = false;
                            throw;
                        }
                        Initialized = true;
                    }
        }

        public static void Init(bool _initialized)
        {
            Initialized = _initialized;
            if (!Initialized)
                lock (lckInit)
                    if (!Initialized)
                    {
                        var currentOperation = string.Empty;

                        InitData();

                        try
                        {
                            InitIniterfaces();

                            Initialized = true;
                        }
                        catch
                        {
                            Initialized = false;
                            throw;
                        }
                        Initialized = true;
                    }
        }

        public static void InitIniterfaces()
        {
            if (!Initialized)
            {
                lock (lckInit)
                    if (!Initialized)
                    {
                        try
                        {
                            var tmpProvider = new ApplicationConfigProvider();
                            var localDataExplorer = new DataExplorer<ApplicationConfig>();
                            localDataExplorer._keyspace = tmpProvider._keyspace;
                            localDataExplorer._columnFamily = tmpProvider._columnFamily;
                            localDataExplorer._keyPrefix = tmpProvider._keyPrefix;

                            Container.RegisterAsForMock<IApplicationConfigProvider>(new ApplicationConfigProvider(localDataExplorer));

                            entities lstIOCs = JSon.Deserialize<entities>(Config.Manager.GetApplicationConfigValue("Entities", "AllApplications"));

                            //Batch = 0
                            //Interactive = 1
                            //LowLatency = 2
                            //SustainedLowLatency = 3
                            System.Runtime.GCSettings.LatencyMode = (System.Runtime.GCLatencyMode)Parser.ToInt(Config.Manager.GetApplicationConfigValue("GCLatencyMode", "AllApplications"), 3);

                            if (lstIOCs != null)
                            {
                                foreach (entity ioc in lstIOCs)
                                {
                                    var _assemblyPath = string.Empty;
                                    Type providerType = null;
                                    Type interfaceType = null;

                                    foreach (var assemblyPath in ioc.assemblyPath.Split(','))
                                    {
                                        if (File.Exists(assemblyPath))
                                            _assemblyPath = assemblyPath;
                                        else if (File.Exists((AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + assemblyPath))
                                            _assemblyPath = (AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + assemblyPath;
                                        else if (File.Exists((AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + "bin\\" + assemblyPath))
                                            _assemblyPath = (AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + "bin\\" + assemblyPath;
                                        else if (File.Exists((AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                           AppDomain.CurrentDomain.BaseDirectory :
                                                           AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                           + "Actions\\" + assemblyPath))
                                            _assemblyPath = (AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + "Actions\\" + assemblyPath;

                                        if (String.IsNullOrEmpty(_assemblyPath)) continue;
                                        if (providerType != null && interfaceType != null) break;

                                        if (providerType == null)
                                            providerType = Assembly.LoadFrom(_assemblyPath).GetType(ioc.providerFullName);
                                        if (interfaceType == null)
                                            interfaceType = Assembly.LoadFrom(_assemblyPath).GetType(ioc.interfaceFullName);

                                        if (providerType == null || interfaceType == null)
                                            throw new Exception("Invalid entities config \"" + ioc.assemblyPath + "," + ioc.fullName + "\"");

                                        if (interfaceType.IsGenericType)
                                        {
                                            interfaceType = interfaceType.MakeGenericType(Assembly.LoadFrom(_assemblyPath).GetType(ioc.fullName));
                                        }
                                    }

                                    if (String.IsNullOrEmpty(_assemblyPath)) continue;

                                    if (providerType == null || interfaceType == null)
                                        throw new Exception("Invalid entities config \"" + ioc.assemblyPath + "," + ioc.fullName + "\"");

                                    try
                                    {
                                        //If Container doesn't have interfaceType defined
                                        if ((typeof(Container).GetMethod("Resolve").MakeGenericMethod(interfaceType)
                                                .Invoke(null, null) == null
                                                && ((providerType.BaseType).FullName.Contains("automation.core.components.data.v1.Cassandra.DataExplorer")
                                                || (providerType.BaseType).FullName.Contains("automation.core.components.data.v1.MSSQL.DataExplorer")
                                                || (providerType.BaseType).FullName.Contains("automation.core.components.data.v1.ElasticSearch.Communicator")
                                                || (providerType.BaseType).FullName.Contains("automation.core.components.data.v1.Redis.DataExplorer"))
                                                || (providerType.BaseType).FullName.Contains("automation.core.components.data.v1.Cassandra.Communicator"))
                                            // add this special case because this provider get invoke incorrectly
                                            || ioc.providerFullName == "aric.variable.v1.manager.provider.cassandra.Provider")
                                        {
                                            string typeName;
                                            object o = null;
                                            var defaultProvider = Activator.CreateInstance(providerType);

                                            if ((providerType.BaseType).FullName.Contains("automation.core.components.data.v1.Cassandra.DataExplorer"))
                                            {
                                                typeName =
                                                    (providerType.BaseType).FullName.Replace(
                                                        "automation.core.components.data.v1.Cassandra.DataExplorer`1[[",
                                                        "").Split(',')[0];

                                                var d1 = typeof(DataExplorer<>);
                                                if (typeName.Contains("System.Collections.Generic.List"))
                                                {
                                                    Type[] typeArgs =
                                                        {
                                                            Type.GetType(
                                                                typeName.Substring(
                                                                    typeName.IndexOf("System.", StringComparison.Ordinal),
                                                                    typeName.Length) + "]]")
                                                        };

                                                    Type[] typeArgs_DiffNamespace =
                                                        {
                                                            Type.GetType(
                                                                typeName.Substring(
                                                                    typeName.IndexOf("System.", StringComparison.Ordinal),
                                                                    typeName.Length) + ", AutomationServices.Entities]]")
                                                        };

                                                    var makeme = d1.MakeGenericType(typeArgs == null || typeArgs[0] == null ? typeArgs_DiffNamespace : typeArgs);
                                                    o = Activator.CreateInstance(makeme);
                                                }
                                                else if (typeName.Contains("AutomationServices.Entities.Process.v25.Process") || typeName.Contains("AutomationServices.Entities.Process.v25.Definition.ActionBase"))
                                                {
                                                    Type[] typeArgs = { Assembly.LoadFrom("AutomationServices.Entities.dll").GetType(typeName) };
                                                    var makeme = d1.MakeGenericType(typeArgs);
                                                    o = Activator.CreateInstance(makeme);
                                                }
                                                else if(typeName.Contains("automation.components.entities.v1.Process.v25.Process") || typeName.Contains("automation.components.entities.v1.Process.v25.Definition.ActionBase"))
                                                {
                                                    Type[] typeArgs = { Assembly.LoadFrom("automation.components.entities.v1.dll").GetType(typeName) };
                                                    var makeme = d1.MakeGenericType(typeArgs);
                                                    o = Activator.CreateInstance(makeme);
                                                }
                                                else if (typeName == "System.Guid")
                                                {
                                                    Type[] typeArgs = { typeof(System.Guid) };
                                                    var makeme = d1.MakeGenericType(typeArgs);
                                                    o = Activator.CreateInstance(makeme);
                                                }
                                                else
                                                {
                                                    Type[] typeArgs = { Assembly.LoadFrom(_assemblyPath).GetType(typeName) };
                                                    var makeme = d1.MakeGenericType(typeArgs);
                                                    o = Activator.CreateInstance(makeme);
                                                }

                                                o.GetType().GetProperty("_keyspace", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                                                    .SetValue(o, defaultProvider.GetType().BaseType.GetProperty("_keyspace", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).GetValue(defaultProvider));
                                                o.GetType().GetProperty("_columnFamily", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                                                    .SetValue(o, defaultProvider.GetType().BaseType.GetProperty("_columnFamily", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).GetValue(defaultProvider));
                                                o.GetType().GetProperty("_keyPrefix", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                                                    .SetValue(o, defaultProvider.GetType().BaseType.GetProperty("_keyPrefix", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).GetValue(defaultProvider));
                                            }

                                            if ((providerType.BaseType).FullName.Contains("automation.core.components.data.v1.Redis.DataExplorer"))
                                            {
                                                typeName =
                                                    (providerType.BaseType).FullName.Replace(
                                                        "automation.core.components.data.v1.Redis.DataExplorer`1[[", "")
                                                        .Split(',')[0];
                                                var d1 = typeof(Local.Redis.DataExplorer<>);
                                                if (typeName.Contains("System.Collections.Hashtable"))
                                                {
                                                    Type[] typeArgs =
                                                        {
                                                            Type.GetType(typeName)
                                                        };

                                                    var makeme = d1.MakeGenericType(typeArgs);
                                                    o = Activator.CreateInstance(makeme);
                                                }
                                                else
                                                {
                                                    Type[] typeArgs = { Assembly.LoadFrom(_assemblyPath).GetType(typeName) };
                                                    var makeme = d1.MakeGenericType(typeArgs);
                                                    o = Activator.CreateInstance(makeme);
                                                }
                                            }
                                            
                                            //if ((providerType.BaseType).FullName.Contains("automation.core.components.data.v1.ElasticSearch.Communicator"))
                                            //{
                                            //    typeName =
                                            //        (providerType.BaseType).FullName.Replace(
                                            //            "automation.core.components.data.v1.ElasticSearch.Communicator`1[[", "")
                                            //            .Split(',')[0];

                                            //    var d1 = typeof(Local.ElasticSearch.Communicator<>);
                                            //    Type[] typeArgs = { Assembly.LoadFrom(_assemblyPath).GetType(typeName) };
                                            //    var makeme = d1.MakeGenericType(typeArgs);
                                            //    o = Activator.CreateInstance(makeme);

                                            //    o.GetType().GetProperty("_indexName", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                                            //       .SetValue(o, defaultProvider.GetType().BaseType.GetProperty("_indexName", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).GetValue(defaultProvider));

                                            //    o.GetType().GetProperty("_typeName", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                                            //      .SetValue(o, defaultProvider.GetType().BaseType.GetProperty("_typeName", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).GetValue(defaultProvider));

                                            //}

                                            #region Communicator Provider
                                            //if ((providerType.BaseType).FullName.Contains("automation.core.components.data.v1.Cassandra.Communicator"))
                                            //{
                                            //    typeName =
                                            //        (providerType.BaseType).FullName.Replace(
                                            //            "automation.core.components.data.v1.Cassandra.Communicator`1[[",
                                            //            "").Split(',')[0];

                                            //    var d1 = typeof(Local.Cassandra.Communicator<>);
                                            //    if (typeName.Contains("System.Collections.Generic.List"))
                                            //    {
                                            //        Type[] typeArgs =
                                            //            {
                                            //                Type.GetType(
                                            //                    typeName.Substring(
                                            //                        typeName.IndexOf("System.", StringComparison.Ordinal),
                                            //                        typeName.Length) + ", AutomationServices.Entities]]")
                                            //            };

                                            //        var makeme = d1.MakeGenericType(typeArgs);
                                            //        o = Activator.CreateInstance(makeme);
                                            //    }
                                            //    // for the fullName and provider/interface are in different namespace cases
                                            //    else if (typeName.Contains("AutomationServices.Entities.Process.v25.Process") || typeName.Contains("AutomationServices.Entities.Process.v25.Definition.ActionBase"))
                                            //    {
                                            //        Type[] typeArgs = { Assembly.LoadFrom("AutomationServices.Entities.dll").GetType(typeName) };
                                            //        var makeme = d1.MakeGenericType(typeArgs);
                                            //        o = Activator.CreateInstance(makeme);
                                            //    }
                                            //    else
                                            //    {
                                            //        Type[] typeArgs = { Assembly.LoadFrom(_assemblyPath).GetType(typeName) };
                                            //        var makeme = d1.MakeGenericType(typeArgs);
                                            //        o = Activator.CreateInstance(makeme);
                                            //    }
                                            //    o.GetType().GetProperty("_keyspace", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                                            //        .SetValue(o, defaultProvider.GetType().BaseType.GetProperty("_keyspace", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).GetValue(defaultProvider));
                                            //    o.GetType().GetProperty("_columnFamily", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                                            //        .SetValue(o, defaultProvider.GetType().BaseType.GetProperty("_columnFamily", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).GetValue(defaultProvider));
                                            //    o.GetType().GetProperty("_keyPrefix", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)
                                            //        .SetValue(o, defaultProvider.GetType().BaseType.GetProperty("_keyPrefix", BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public).GetValue(defaultProvider));
                                            //}
                                            #endregion

                                            typeof(Container).GetMethod("RegisterAsForMock").MakeGenericMethod(interfaceType)
                                                .Invoke(null, new object[] { Activator.CreateInstance(providerType, new object[] { o }) });
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        throw new Exception("Invalid entities config \"" + ioc.assemblyPath + ", " + ioc.fullName + ", " + ioc.providerFullName + ", " + ioc.interfaceFullName + "\"", ex);
                                    }
                                }
                            }

                            //Time Server Intit - BEGIN
                            var timeServer = Config.Manager.GetApplicationConfigValue("TimeServer", "AllApplications");
                            FluentCassandra.TimestampHelper.UtcNow = () => DateTimeExtension.GetTimeServerUTC(timeServer);
                            //Time Server Intit - END

                            //Email Intit - BEGIN
                            Email.SetConfig(Config.Manager.GetApplicationConfigValue("ValidAddresses", "AllApplications.Email"),
                                            Config.Manager.GetApplicationConfigValue("DefaultToAddress", "AllApplications.Email"),
                                            Config.Manager.GetApplicationConfigValue("DefaultSMTPServer", "AllApplications.Email"),
                                            Config.Manager.GetApplicationConfigValue("DefaultFooter", "AllApplications.Email"));
                            //Email Intit - END

                            //DefaultConnectionLimit from application
                            ServicePointManager.DefaultConnectionLimit = 100000;

                            //Accepts All Certificates will have to be removed once we have Certificates Upload feature - This Feature is removed
                            ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, errors) => true;

                            //Allowed Secured Protocols
                            //ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;

                            Initialized = true;
                        }
                        catch
                        {
                            Initialized = false;

                            throw;
                        }
                    }
            }
        }

        private static void InitData()
        {
            foreach (string curdatapath in Directory.GetFiles("AppData", "*.json", SearchOption.AllDirectories))
            {
                if (curdatapath.EndsWith(".wide.json") || curdatapath.EndsWith("redis.index.json") || curdatapath.EndsWith("redis.json") ||
                    curdatapath.EndsWith(".custom.json")) continue;

                var curdatafi = new FileInfo(curdatapath);
                var cfKey = curdatafi.Directory.Name + "." + curdatafi.Name.Replace(".json", "");

                var curdata = JSon.Deserialize<Dictionary<string, object>>(File.ReadAllText(curdatapath));
                if (curdata == null) continue;

                var ds = new Dictionary<string, Dictionary<string, string>>();

                foreach (var currow in curdata)
                    ds.Add(currow.Key, new Dictionary<string, string> { { "json", JSon.Serialize(currow.Value) } });

                data.Add(cfKey, ds);
            }

            foreach (string curdatapath in Directory.GetFiles("AppData", "*elastic.json", SearchOption.AllDirectories))
            {
                //if (curdatapath.EndsWith(".wide.json")) continue;

                var curdatafi = new FileInfo(curdatapath);
                var cfKey = curdatafi.Name.Replace(".elastic.json", "");

                var curdata = JSon.Deserialize<Dictionary<string, object>>(File.ReadAllText(curdatapath));
                if (curdata == null) continue;

                var ds = new Dictionary<string, Dictionary<string, string>>();

                foreach (var currow in curdata)
                    ds.Add(currow.Key, new Dictionary<string, string> { { "json", JSon.Serialize(currow.Value) } });

                data.Add(cfKey, ds);
            }

            foreach (string curdatapath in Directory.GetFiles("AppData", "*.wide.json", SearchOption.AllDirectories))
            {
                var curdatafi = new FileInfo(curdatapath);
                var cfKey = curdatafi.Directory.Name + "." + curdatafi.Name.Replace(".wide.json", "");

                var curdata = JSon.Deserialize<Dictionary<string, Dictionary<string, string>>>(File.ReadAllText(curdatapath));
                if (curdata == null) continue;

                var ds = new Dictionary<string, Dictionary<string, string>>();

                foreach (var currow in curdata)
                    foreach (var curindexrow in currow.Value)
                    {
                        if (!ds.ContainsKey(currow.Key))
                            ds.Add(currow.Key, new Dictionary<string, string> { { curindexrow.Key, curindexrow.Value } });
                        else
                        {
                            Dictionary<string, string> value;
                            ds.TryGetValue(currow.Key, out value);

                            if (value != null && !value.ContainsKey(curindexrow.Key))
                                value.Add(curindexrow.Key, curindexrow.Value);

                            ds[currow.Key] = value;
                        }
                    }

                data.Add(cfKey, ds);
            }

            foreach (string curdatapath in Directory.GetFiles("AppData", "*.custom.json", SearchOption.AllDirectories))
            {
                var curdatafi = new FileInfo(curdatapath);
                var cfKey = curdatafi.Directory.Name + "." + curdatafi.Name.Replace(".custom.json", "");

                var table = JContainer.Parse(File.ReadAllText(curdatapath));
                if (table == null || table["Definition"] == null || table["Rows"] == null) continue;

                var tableDef = JContainer.Parse(table["Definition"].ToString());
                if (tableDef["Columns"] == null || tableDef["Primary Key"] == null ||
                    tableDef["Columns"].ToString().Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Length <= 0 ||
                    tableDef["Primary Key"].ToString().Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Length <= 0) continue;

                Dictionary<string, Dictionary<string, string>> rows = new
                    Dictionary<string, Dictionary<string, string>>{{"key", new Dictionary<string, string>{{"json", table.ToString()}}}};;

                data.Add(cfKey, rows);
            }

            foreach (string curdatapath in Directory.GetFiles("AppData", "*.sql", SearchOption.AllDirectories))
            {
                var curdatafi = new FileInfo(curdatapath);
                var tableName = curdatafi.Name.Replace(".sql", "");

                sqlData.Tables.Add(ConvertToDataTable(curdatapath, tableName, ";"));
            }
            foreach (string curdatapath in Directory.GetFiles("AppData", "redis.json", SearchOption.AllDirectories))
            {
                var curdatafi = new FileInfo(curdatapath);
                var curdata = JSon.Deserialize<Dictionary<string, string>>(File.ReadAllText(curdatapath));
                if (curdata == null) continue;

                foreach (var currow in curdata)
                    redisData.Add(currow.Key, currow.Value);
            }
            foreach (string curdatapath in Directory.GetFiles("AppData", "redis.index.json", SearchOption.AllDirectories))
            {
                var curdatafi = new FileInfo(curdatapath);
                var curdata = JSon.Deserialize<Dictionary<string, Dictionary<string, string>>>(File.ReadAllText(curdatapath));
                if (curdata == null) continue;

                foreach (var currow in curdata)
                    redisIndexData.Add(currow.Key, currow.Value);
            }
        }

        private static Cassandra.IDataExplorer<ApplicationConfig> DataExplorer<T1>()
        {
            throw new NotImplementedException();
        }

        public static DataTable ConvertToDataTable(string file, string tableName, string delimiter)
        {
            var result = new DataTable(tableName);

            var s = new StreamReader(file);

            var readLine = s.ReadLine();
            if (readLine != null)
            {
                var columns = readLine.Split(delimiter.ToCharArray());

                foreach (var col in columns)
                {
                    var added = false;
                    var next = "";
                    var i = 0;
                    while (!added)
                    {
                        var columnname = col + next;
                        columnname = columnname.Replace("#", "");
                        columnname = columnname.Replace("'", "");
                        columnname = columnname.Replace("&", "");

                        if (!result.Columns.Contains(columnname))
                        {
                            result.Columns.Add(columnname);
                            added = true;
                        }
                        else
                        {
                            i++;
                            next = "_" + i.ToString(CultureInfo.InvariantCulture);
                        }
                    }
                }
            }

            var allData = s.ReadToEnd();

            var rows = allData.Split("\r\n".ToCharArray());

            foreach (var items in rows.Select(r => r.Split(delimiter.ToCharArray())))
            {
                result.Rows.Add(items);
            }

            return result;
        }
    }

    public class EmailMockClient : Email.IClient
    {
        public Dictionary<string, MailMessage> EmailList = new Dictionary<string, MailMessage>();

        public void Send(string Host, MailMessage Email)
        {
            if (!EmailList.ContainsKey(Email.Subject))
                EmailList.Add(Email.Subject, Email);
            else
                EmailList[Email.Subject] = Email;
        }
    }
}