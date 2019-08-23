using System.Collections.Generic;
using System.Configuration;
using System.Linq;

namespace automation.components.data.v1.AppContainer
{
    /// <summary>
    /// Provides a list of entities from the xml app.config/web.config for an application.
    /// </summary>
    public class XmlConfigEntitiesProvider : IEntitiesProvider
    {
        /// <summary>
        /// Get entities from xml config
        /// </summary>
        /// <returns>List of entities as defined in app.config/web.config for the application</returns>
        public IEnumerable<Entity> GetEntities()
        {
            CustomEntities section = ConfigurationManager.GetSection("entities") as CustomEntities;
            CustomEntity entityElements = section?.CustomEntity ?? new CustomEntity();
            return entityElements.OfType<Element>().Select(e => e.ToEntity());
        }
    }
}