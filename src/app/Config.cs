using System;
using System.Collections.Generic;
using automation.core.components.data.v1.Providers;
using System.Configuration;

namespace automation.core.components.data.v1.Config
{
    public static class Manager
    {
        internal static object ClientCacheLock = new object();

        public static string GetApplicationConfigValue(string name, string nameSpace)
        {            
            var config = GetApplicationConfig(name, nameSpace, GetSystemType());

            if (config == default(ApplicationConfig))
            {
                var specialCases = new List<string> { "PCE.Actions.ExecutedFlow.RBAN-2732", "PCE.Actions.ExecutedFlow.Logs.RBAN-2732", "RBAN-4065_AMV_SwapKey" };
                if (specialCases.Contains(name))
                {
                    return "on";
                }
                return string.Empty;
            }

            return config.Value;
        }

        public static ApplicationConfig GetApplicationConfig(string name, string nameSpace)
        {
            return GetApplicationConfig(name, nameSpace, GetSystemType());
        }

        public static ApplicationConfig GetApplicationConfig(string name, string nameSpace, string environment)
        {
            var client = Container.Resolve<IApplicationConfigProvider>();

            if (client == null)
            {
                Container.RegisterAs(typeof(IApplicationConfigProvider));
                client = Container.Resolve<IApplicationConfigProvider>();
            }

            return GetApplicationConfig(client, name, nameSpace, environment);
        }

        private static Dictionary<string, ApplicationConfigBuffer> bufferConfig = new Dictionary<string, ApplicationConfigBuffer>();
        private static readonly object BufferConfigLock = new object();
        public static ApplicationConfig GetApplicationConfig(IApplicationConfigProvider provider,
                                                             string name,
                                                             string nameSpace,
                                                             string environment)
        {
            string bufferKey = string.Format("{0}:{1}:{2}", environment, nameSpace, name);
            lock (BufferConfigLock)
            {
                if (bufferConfig.ContainsKey(bufferKey))
                {
                    var bufferedConfig = bufferConfig[bufferKey];

                    if (bufferedConfig != default(ApplicationConfigBuffer) &&
                        bufferedConfig.ApplicationConfig != default(ApplicationConfig) &&
                        bufferedConfig.CacheUntil != default(DateTime) &&
                        bufferedConfig.CacheUntil > DateTime.UtcNow)
                        return bufferedConfig.ApplicationConfig;
                }
            }

            //If not cached get it from data store
            var curConfig = provider.GetApplicationConfig(name, nameSpace, environment);

            if (curConfig != default(ApplicationConfig) &&
                curConfig.CacheTime != default(TimeSpan) &&
                curConfig.CacheTime.TotalMilliseconds > 0)
                lock (BufferConfigLock)
                {
                    if (bufferConfig.ContainsKey(bufferKey))
                        bufferConfig[bufferKey] = new ApplicationConfigBuffer
                        {
                            ApplicationConfig = curConfig,
                            CacheUntil = DateTime.UtcNow.Add(curConfig.CacheTime)
                        };
                    else if (!bufferConfig.ContainsKey(bufferKey))
                        bufferConfig.Add(bufferKey,
                            new ApplicationConfigBuffer
                            {
                                ApplicationConfig = curConfig,
                                CacheUntil = DateTime.UtcNow.Add(curConfig.CacheTime)
                            });
                }

            return curConfig;
        }

        private static string defaultEnvironment = "";
        /// <summary>
        /// Get Virtual Environment
        /// </summary>
        /// <returns>Default Virtual Environment</returns>
        public static string GetEnvironment()
        {
            if (string.IsNullOrEmpty(defaultEnvironment))
                defaultEnvironment = ConfigurationManager.AppSettings["environment"];

            return defaultEnvironment;
        }

        /// <summary>
        /// Get Virtual Environment
        /// </summary>
        /// <param name="environment">From IAppEntity</param>
        /// <returns>Valid Virtual Environment, or Default Virtual Environment</returns>
        public static string GetEnvironment(string environment)
        {
            if (!String.IsNullOrEmpty(environment))
                return environment.ToLower();

            return GetEnvironment();
        }

        private static string systemType = "";
        private static DateTime systemTypeExpiresAt;
        public static string GetSystemType()
        {
            if (string.IsNullOrEmpty(systemType) || systemTypeExpiresAt < DateTime.UtcNow)
            {
                systemType = ConfigurationManager.AppSettings["SystemType"] ?? "";
                if (string.IsNullOrEmpty(systemType))
                {
                    var cutOffDate = new DateTime(2014, 1, 1);
                    if (DateTime.Compare(DateTime.Now, cutOffDate) > 0)
                    {
                        throw new Exception("SystemType config is missing in application config");
                    }
                    var client = Container.Resolve<IApplicationConfigProvider>();
                    systemType = GetSystemType(client);
                }
                systemTypeExpiresAt = DateTime.UtcNow.AddHours(1);
            }

            return systemType;
        }

        public static string GetSystemType(IApplicationConfigProvider provider)
        {
            var config = provider.GetApplicationConfigByName("SystemType");
            return config == null ? string.Empty : config.Value;
        }

        public static bool UpdateApplicationConfig(ApplicationConfig applicationConfig)
        {
            var client = Container.Resolve<IApplicationConfigProvider>();
            return client.SetApplicationConfig(applicationConfig);
        }

        public static Int32 GetSecondsFromLastSync<T>()
        {
            return 0;
        }

        public static void ClearBuffer()
        {
            lock (BufferConfigLock)
                bufferConfig = new Dictionary<string, ApplicationConfigBuffer>();
        }
    }
}
