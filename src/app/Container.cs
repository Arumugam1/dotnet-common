using automation.core.components.data.v1.Providers;
using automation.core.components.data.v1.Providers.Cassandra;
using automation.core.components.operations.v1;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading;

namespace automation.core.components.data.v1
{
    public static class Container
    {
        private static Funq.Container container { get; set; }
        private static bool initialized = false;
        private static object lckInit = new object();
        private static object _containerLock = new object();

        private static Funq.Container CustomContainer { get; set; }
        private static bool initializedConfig = false;
        private static object lckInitConfig = new object();
        private static object _customContainerLock = new object();

        public static void Init()
        {
            if (!initialized)
                lock (lckInit)
                    if (!initialized)
                    {
                        try
                        {
                            container = new Funq.Container();

                            Container.RegisterAsInternal<ApplicationConfigProvider, IApplicationConfigProvider>();

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
                                    var assemblyPath = string.Empty;
                                    Type providerType = null;
                                    Type interfaceType = null;

                                    foreach (var location in ioc.assemblyPath.Split(','))
                                    {
                                        if (File.Exists(location))
                                            assemblyPath = location;
                                        else if (File.Exists((AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + location))
                                            assemblyPath = (AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + location;
                                        else if (File.Exists((AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + "bin\\" + location))
                                            assemblyPath = (AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + "bin\\" + location;
                                        else if (File.Exists((AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                           AppDomain.CurrentDomain.BaseDirectory :
                                                           AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                           + "Actions\\" + location))
                                            assemblyPath = (AppDomain.CurrentDomain.BaseDirectory.EndsWith("\\") ?
                                                            AppDomain.CurrentDomain.BaseDirectory :
                                                            AppDomain.CurrentDomain.BaseDirectory + "\\")
                                                            + "Actions\\" + location;

                                        if (String.IsNullOrEmpty(assemblyPath)) continue;
                                        if (providerType != null && interfaceType != null) break;

                                        if (providerType == null)
                                            providerType = Assembly.LoadFrom(assemblyPath).GetType(ioc.providerFullName);
                                        if (interfaceType == null)
                                            interfaceType = Assembly.LoadFrom(assemblyPath).GetType(ioc.interfaceFullName);

                                        if (providerType == null || interfaceType == null)
                                            throw new Exception("Invalid entities config \"" + ioc.assemblyPath + "," + ioc.fullName + "\"");

                                        if (interfaceType.IsGenericType)
                                        {
                                            interfaceType = interfaceType.MakeGenericType(Assembly.LoadFrom(assemblyPath).GetType(ioc.fullName));
                                        }
                                    }

                                    if (String.IsNullOrEmpty(assemblyPath)) continue;

                                    if (providerType == null || interfaceType == null)
                                        throw new Exception("Invalid entities config \"" + ioc.assemblyPath + "," + ioc.fullName + "\"");

                                    try
                                    {
                                        //If Container doesn't have interfaceType defined
                                        if (typeof(Container).GetMethod("Resolve").MakeGenericMethod(interfaceType)
                                            .Invoke(null, null) == null)
                                        {
                                            typeof(Container).GetMethod("RegisterAs").MakeGenericMethod(interfaceType)
                                                .Invoke(null, new object[] { Activator.CreateInstance(providerType) });
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        throw new Exception("Invalid entities config \"" + ioc.assemblyPath + "," + ioc.fullName + "\"", ex);
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

                            initialized = true;
                        }
                        catch
                        {
                            container.Dispose();
                            initialized = false;

                            throw;
                        }
                    }
        }

        public static void Register<T, TAs>() where T : TAs
        {
            try
            {
                Register<T, TAs>((T)Activator.CreateInstance(typeof(T)));
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("No parameterless constructor defined for this object."))
                    throw new Exception("No parameterless constructor defined for this object, Please use \"Register<T, TAs>(T instance)\" or \"RegisterAs<TAs>(TAs instance)\"", ex);
                throw;
            }
        }

        public static void Register<T, TAs>(T instance) where T : TAs
        {
            RegisterAs<TAs>(instance);
        }

        public static void RegisterAs<TAs>(TAs instance)
        {
            if (IsCustomRegisterDisabled() && initialized)
                throw new Exception("The method : RegisterAs has been deprecated. please use contain.init() to initialize your dependency");

            RegisterAsInternal<TAs>(instance);
        }

        /// <summary>
        /// Register the component
        /// </summary>
        /// <typeparam name="TAs"></typeparam>
        /// <param name="instance"></param>
        private static void RegisterAsInternal<TAs>(TAs instance)
        {
            if (!initialized && !Monitor.IsEntered(lckInit))
                Init();

            lock (_containerLock)
                container.Register<TAs>(instance);
        }

        /// <summary>
        /// Register the component
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TAs"></typeparam>
        private static void RegisterAsInternal<T, TAs>() where T : TAs
        {
            RegisterAsInternal<TAs>((T)Activator.CreateInstance(typeof(T)));
        }

        public static void RegisterAsForMock<TAs>(TAs instance)
        {
            if (container == null)
            {
                container = new Funq.Container();
                initialized = true;
            }

            container.Register<TAs>(instance);
        }

        public static T Resolve<T>()
        {
            if (container == null)
                return default(T);

            return container.TryResolve<T>();
        }

        public static void Reload()
        {
            initialized = false;
            Init();
        }

        public static void InitializeConfig()
        {
            if (!initializedConfig)
                lock (lckInitConfig)
                    if (!initializedConfig)
                    {
                        try
                        {
                            CustomContainer = new Funq.Container();

                            var section  = ConfigurationManager.GetSection("entities");

                            CustomEntity lstIOCs = (section as CustomEntities).CustomEntity;;

                            if (lstIOCs != null && lstIOCs.Count > 0)
                            {
                                foreach (Element ioc in lstIOCs)
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
                                            throw new Exception("Invalid custom entities config \"" + ioc.assemblyPath + "," + ioc.fullName + "\"");

                                        if (interfaceType.IsGenericType)
                                        {
                                            interfaceType = interfaceType.MakeGenericType(Assembly.LoadFrom(_assemblyPath).GetType(ioc.fullName));
                                        }
                                    }

                                    if (String.IsNullOrEmpty(_assemblyPath)) continue;

                                    if (providerType == null || interfaceType == null)
                                        throw new Exception("Invalid custom entities config \"" + ioc.assemblyPath + "," + ioc.fullName + "\"");

                                    try
                                    {
                                        if (typeof(Container).GetMethod("ResolveCustom").MakeGenericMethod(interfaceType)
                                            .Invoke(null, null) == null)
                                        {
                                            typeof(Container).GetMethod("RegisterAsCustom").MakeGenericMethod(interfaceType)
                                                .Invoke(null, new object[] { Activator.CreateInstance(providerType) });
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        throw new Exception("Invalid entities config \"" + ioc.assemblyPath + "," + ioc.fullName + "\"", ex);
                                    }
                                }
                            }

                            //DefaultConnectionLimit from application
                            ServicePointManager.DefaultConnectionLimit = 100000;

                            //Accepts All Certificates will have to be removed once we have Certificates Upload feature - This Feature is removed
                            ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, errors) => true;

                            initializedConfig = true;
                        }
                        catch
                        {
                            CustomContainer.Dispose();
                            initializedConfig = false;

                            throw;
                        }
                    }
        }

        public static void RegisterCustom<T, TAs>() where T : TAs
        {
            try
            {
                RegisterCustom<T, TAs>((T)Activator.CreateInstance(typeof(T)));
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("No parameterless constructor defined for this object."))
                    throw new Exception("No parameterless constructor defined for this object, Please use \"Register<T, TAs>(T instance)\" or \"RegisterCustom<TAs>(TAs instance)\"", ex);
                throw;
            }
        }

        public static void RegisterCustom<T, TAs>(T instance) where T : TAs
        {
            RegisterAsCustom<TAs>(instance);
        }

        public static void RegisterAsCustom<TAs>(TAs instance)
        {
            if (!initializedConfig && !Monitor.IsEntered(lckInitConfig))
                InitializeConfig();

            lock(_customContainerLock)
                CustomContainer.Register<TAs>(instance);
        }

        public static T ResolveCustom<T>()
        {
            if (CustomContainer == null)
                return default(T);

            return CustomContainer.TryResolve<T>();
        }

        public static void ReloadConfig()
        {
            initializedConfig = false;
            InitializeConfig();
        }

        /// <summary>
        /// check to see the feature flag is enabled or not
        /// </summary>
        /// <returns></returns>
        private static bool IsCustomRegisterDisabled()
        {
            bool isDisabled = false;
            try
            {
                string appName = ConfigurationManager.AppSettings["ServiceExecutor.AppName"] != null ?
                    string.Format("{0}.",ConfigurationManager.AppSettings["ServiceExecutor.AppName"]) : "";

                if (Container.Resolve<IApplicationConfigProvider>() == null)
                    Container.RegisterAsInternal<ApplicationConfigProvider, IApplicationConfigProvider>();

                isDisabled = Parser.ToBoolean(Config.Manager.GetApplicationConfigValue(string.Format("{0}CustomRegisterDisabled", appName), "AllApplications"), false);
            }
            catch (Exception ex)
            {
                Trace.Exception(TraceTypes.AccessLayer, "automation.core.components.data.v1.Container", "IsCustomRegisterDisabled", new object[] {  }, ex);
            }
            return isDisabled;
        }
    }

    public class entities : List<entity>
    {
    }

    public class entity
    {
        [ConfigurationProperty("assemblyPath", IsRequired = true)]
        public string assemblyPath { get; set; }

        [ConfigurationProperty("fullName", IsRequired = true)]
        public string fullName { get; set; }

        [ConfigurationProperty("providerFullName", IsRequired = true)]
        public string providerFullName { get; set; }

        [ConfigurationProperty("interfaceFullName", IsRequired = true)]
        public string interfaceFullName { get; set; }
    }

    public class CustomEntities : ConfigurationSection
    {
        public const string sectionName = "entities";

        [ConfigurationProperty("", IsDefaultCollection = true)]
        public CustomEntity CustomEntity
        {
            get
            {
                return this[""] as CustomEntity;
            }
        }

        public static CustomEntity GetSection()
        {
            return (CustomEntity)ConfigurationManager.GetSection(sectionName);
        }
    }

    public class CustomEntity : ConfigurationElementCollection
    {
        public CustomEntity()
        {
            this.AddElementName = "entity";
        }

        protected override object GetElementKey(ConfigurationElement element)
        {
            return (element as Element).assemblyPath;
        }

        protected override ConfigurationElement CreateNewElement()
        {
            return new Element();
        }

        public new Element this[string key]
        {
            get { return base.BaseGet(key) as Element; }
        }

        public Element this[int ind]
        {
            get { return base.BaseGet(ind) as Element; }
        }
    }

    public class Element : ConfigurationElement
    {
        private const string AttributeAssemblyPath = "assemblyPath";
        private const string AttributeFullName = "fullName";
        private const string AttributeProviderFullName = "providerFullName";
        private const string AttributeInterfaceFullName = "interfaceFullName";

        [ConfigurationProperty(AttributeAssemblyPath, IsRequired = true, IsKey = true)]
        public string assemblyPath
        {
            get { return (string)this[AttributeAssemblyPath]; }
            set { this[AttributeAssemblyPath] = value; }
        }

        [ConfigurationProperty(AttributeFullName, IsRequired = true, IsKey = false)]
        public string fullName
        {
            get { return (string)this[AttributeFullName]; }
            set { this[AttributeFullName] = value; }
        }

        [ConfigurationProperty(AttributeProviderFullName, IsRequired = true, IsKey = false)]
        public string providerFullName
        {
            get { return (string)this[AttributeProviderFullName]; }
            set { this[AttributeProviderFullName] = value; }
        }

        [ConfigurationProperty(AttributeInterfaceFullName, IsRequired = true, IsKey = false)]
        public string interfaceFullName
        {
            get { return (string)this[AttributeInterfaceFullName]; }
            set { this[AttributeInterfaceFullName] = value; }
        }
    }

}