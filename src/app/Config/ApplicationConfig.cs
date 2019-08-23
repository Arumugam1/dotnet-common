using System;

namespace automation.components.data.v1.Config
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

        public string Key()
        {
            return string.Format("{0}.{1}.{2}", this.Environment, this.Namespace, this.Name);
        }

        public static string Key(string environment, string nameSpace, string name)
        {
            return string.Format("{0}.{1}.{2}", environment, nameSpace, name);
        }

        /// <summary>
        /// Returns a new ConfigNotFound that represents the config item was
        /// not found in the database. So that repeated queries for missing
        /// config can avoid subsequent queries to the db.
        /// </summary>
        /// <returns>ConfigNotFound empty config object</returns>
        internal static ConfigNotFound NotFound() => new ConfigNotFound();

        internal sealed class ConfigNotFound : ApplicationConfig { }
    }
}
