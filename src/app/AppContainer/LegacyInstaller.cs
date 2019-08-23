using System;
using System.Collections.Generic;
using System.Reflection;
using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

namespace automation.components.data.v1.AppContainer
{
    /// <summary>
    /// Responsible for creating and initializing a Castle Windsor container.
    ///
    /// It can be configured with an entitiesProvider which will return the list of
    /// entities to register in the container that gets returned.
    /// </summary>
    public class LegacyInstaller : IWindsorInstaller
    {
        private readonly IEntitiesProvider entitiesProvider;
        private readonly IEnumerable<string> assemblyDirs = new string[]
        {
            "bin",
            "Actions"
        };

        /// <summary>
        /// Initializes a new instance of the <see cref="LegacyInstaller"/> class.
        /// </summary>
        /// <param name="entitiesProvider">entities provider that will determine which entities get
        /// registered on new containers created by this factory</param>
        public LegacyInstaller(IEntitiesProvider entitiesProvider)
        {
            this.entitiesProvider = entitiesProvider;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="LegacyInstaller"/> class.
        ///
        /// Convenience contructore to avoid the need to create an entities provider
        /// that returns an empty list of entities.  If this is used the new
        /// container will have no entities registered.
        /// </summary>
        public LegacyInstaller() {}

        /// <summary>
        /// Installs the entities provided by the IEntitiesProvider into the supplied
        /// application container.
        /// </summary>
        /// <param name="container"></param>
        /// <param name="store"></param>
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            var entities = this.entitiesProvider?.GetEntities() ?? new List<Entity>();
            this.RegisterEntities(entities, container);
        }

        private void RegisterEntities(IEnumerable<Entity> entities, IWindsorContainer appContainer)
        {
            foreach (Entity e in entities)
            {
                string fullPath = FileUtils.FindFirst(e.assemblyPath, this.assemblyDirs);
                if (fullPath != null)
                {
                    Type providerType = this.ResolveType(fullPath, e.providerFullName);
                    Type interfaceType = this.ResolveType(fullPath, e.interfaceFullName);
                    appContainer.Register(Component.For(interfaceType).ImplementedBy(providerType).IsFallback().OnlyNewServices());
                }
            }
        }

        private Type ResolveType(string assemblyFullPath, string typeName)
        {
            Type type = Assembly.LoadFrom(assemblyFullPath).GetType(typeName);
            if (type == null)
            {
                var fmtString = "Container failed to resolve entity type: [ {0} ] in Assembly: {1}";
                var msg = string.Format(fmtString, typeName, assemblyFullPath);
                throw new ContainerConfigException(msg);
            }

            return type;
        }
    }
}
