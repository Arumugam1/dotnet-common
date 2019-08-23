using System.Collections.Generic;

namespace automation.components.data.v1.AppContainer
{
    public interface IEntitiesProvider
    {
        IEnumerable<Entity> GetEntities();
    }
}
