using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Runtime.Caching;
using System.Text.RegularExpressions;
using automation.components.data.v1.Providers;

namespace automation.components.data.v1.Config
{
    /// <summary>
    /// Class for managing application config which includes caching values retreived from the 
    /// database.
    /// </summary>
    public static class Manager
    {
        private static readonly MemoryCache configCache = new MemoryCache("ApplicationConfigCache");

        private static readonly Lazy<string> systemType = new Lazy<string>(() =>
        {
            var type = ConfigurationManager.AppSettings["SystemType"];
            if (string.IsNullOrEmpty(type))
            {
                throw new Exception("SystemType config is missing in application config");
            }
            return type;
        });

        private static readonly Lazy<IApplicationConfigProvider> configProvider = new Lazy<IApplicationConfigProvider>(() =>
        {
            var provider = Container.Resolve<IApplicationConfigProvider>();
            if (provider == null)
            {
                Container.Init();
                provider = Container.Resolve<IApplicationConfigProvider>();
            }
            return provider;
        });

        private static readonly string defaultEnvironment = ConfigurationManager.AppSettings["environment"];

        private static readonly Random generator = new Random();

        private static readonly IEnumerable<Regex> cacheBlackList = new Regex[]
        {
            new Regex(@".*session$")
        };

        /// <summary>
        /// Get an application configs 'value' field as a string
        /// </summary>
        /// <param name="name">name of application config to return</param>
        /// <param name="nameSpace">namespace of application config to return</param>
        /// <returns>ApplicationConfig.value or string.Empty if not found</returns>
        public static string GetApplicationConfigValue(string name, string nameSpace)
        {
            var config = GetApplicationConfig(name, nameSpace, systemType.Value);

            return config?.Value ?? string.Empty;
        }

        /// <summary>
        /// Get the whole ApplicationConfig object.
        /// </summary>
        /// <param name="name">name of application config to return</param>
        /// <param name="nameSpace">namespace of application config to return</param>
        /// <returns>application config or null if not found</returns>
        public static ApplicationConfig GetApplicationConfig(string name, string nameSpace)
        {
            return GetApplicationConfig(name, nameSpace, systemType.Value);
        }

        /// <summary>
        /// Get application config object for any environment
        /// </summary>
        /// <param name="name">name of application config to return</param>
        /// <param name="nameSpace">namespace of application config to return</param>
        /// <param name="environment">environment of application config to return</param>
        /// <returns>matching application config object or null</returns>
        [Obsolete("this method should not be used outide this class and should be private. There should be no reason" +
            "to access config outside your own environment from client code")]
        public static ApplicationConfig GetApplicationConfig(string name, string nameSpace, string environment)
        {
            return GetApplicationConfig(configProvider.Value, name, nameSpace, environment);
        }

        /// <summary>
        /// Get application config object for any environment
        /// </summary>
        /// <param name="name">name of application config to return</param>
        /// <param name="nameSpace">namespace of application config to return</param>
        /// <param name="environment">environment of application config to return</param>
        /// <param name="provider">config provider implementing dataexplorer for db access</param>
        /// <returns>matching application config object or null</returns>
        [Obsolete("this method should not be used outide this class and should be private. There should be no reason" +
            "to access config outside your own environment from client code and other than mocking no reason to specify" +
            "another provider")]
        public static ApplicationConfig GetApplicationConfig(
            IApplicationConfigProvider provider,
            string name,
            string nameSpace,
            string environment)
        {
            var configCacheKey = ApplicationConfig.Key(environment, nameSpace, name);
            var cached = configCache.Get(configCacheKey);

            if (cached == null)
            {
                var currentConfig = provider?.GetApplicationConfig(name, nameSpace, environment);
                var toCache = currentConfig ?? ApplicationConfig.NotFound();

                configCache.Add(configCacheKey, toCache, GetExpiration(configCacheKey, currentConfig));

                return currentConfig;
            }
            else
            {
                return (cached is ApplicationConfig.ConfigNotFound) ? null : cached as ApplicationConfig;
            }
        }

        /// <summary>
        /// Get Virtual Environment
        /// </summary>
        /// <returns>Default Virtual Environment</returns>
        public static string GetEnvironment() => defaultEnvironment;

        /// <summary>
        /// Get Virtual Environment
        /// </summary>
        /// <param name="environment">From IAppEntity</param>
        /// <returns>Valid Virtual Environment, or Default Virtual Environment</returns>
        public static string GetEnvironment(string environment)
        {
            if (string.IsNullOrEmpty(environment))
            {
                return GetEnvironment();
            }
            else
            {
                return environment.ToLower();
            }
        }

        /// <summary>
        /// Returned the poorly named system type which is really more like
        /// Environment, but different somehow. System time is what maps to
        /// the "environment" field in the ApplicationConfig object and
        /// corresponding cassandra column family.
        /// </summary>
        /// <returns>system type string </returns>
        public static string GetSystemType() => systemType.Value;

        /// <summary>
        /// System type should come from from app.config/web.config and should
        /// NEVER be pulled from another source.  This is a legacy method.
        /// </summary>
        /// <returns>system type string </returns>
        [Obsolete("Never use this.  Use GetSystemType() instead")]
        public static string GetSystemType(IApplicationConfigProvider provider)
        {
            var config = provider.GetApplicationConfigByName("SystemType");
            return config == null ? string.Empty : config.Value;
        }

        /// <summary>
        /// Updates a config value in local cache and in the underlying db/provider.
        /// Values in other caches in other app domains will be unaffected.  Use with
        /// Caution.
        /// </summary>
        /// <param name="applicationConfig">new config value to set</param>
        /// <returns>true if success, false otherwise</returns>
        [Obsolete("Application config should be read only.  For places where we are overloading" +
            "app config and using it for other things like auth tokens, we should move those to" +
            "their own services/providers that do not involve app config")]
        public static bool UpdateApplicationConfig(ApplicationConfig applicationConfig)
        {
            var expiration = GetExpiration(applicationConfig.Key(), applicationConfig);
            configCache.Set(applicationConfig.Key(), applicationConfig, expiration);
            return configProvider.Value.SetApplicationConfig(applicationConfig);
        }

        /// <summary>
        /// removes all cache items.  From reading online trim(100) isn't 
        /// guaranteed to remove everything and we should not rely on this method
        /// it is marked obsolete for a reason and will likely be removed soon.
        /// </summary>
        [Obsolete("Memory Cache does not support a native 'clear' method. " +
            " This is only used by a method of Queuebase that is likely not used")]
        public static void ClearBuffer() => configCache.Trim(100);

        private static DateTimeOffset GetExpiration(string key, ApplicationConfig configItem)
        {
            if (configItem?.CacheTime == null || configItem.CacheTime.TotalMilliseconds == 0)
            {
                if (IsBlackListed(key))
                {
                    return DateTimeOffset.UtcNow;
                }
                else
                {
                    return DefaultExpiration();
                }
            }
            else
            {
                return DateTimeOffset.UtcNow + configItem.CacheTime;
            }
        }

        private static DateTimeOffset DefaultExpiration()
        {
            var minutes = generator.Next(4, 6);
            var seconds = generator.Next(0, 60);

            return DateTimeOffset.UtcNow + new TimeSpan(0, minutes, seconds);
        }

        private static bool IsBlackListed(string key) => cacheBlackList.Any(x => x.IsMatch(key));
    }
}
