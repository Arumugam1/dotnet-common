using automation.components.container;
using automation.components.data.v1.Cassandra;
using automation.components.data.v1.Config;
using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;
using System;

namespace automation.components.data.v1.installers
{
    public class MockEdgeInstaller : IWindsorInstaller, IMockEdgeComponents
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component
                .For<IDataExplorer<ApplicationConfig>>()
                .ImplementedBy<Local.Cassandra.DataExplorer<ApplicationConfig>>()
                .DependsOn(Dependency.OnValue("keyspace", "applicationconfig"))
                .DependsOn(Dependency.OnValue("columnFamily", "applicationconfig"))
                .DependsOn(Dependency.OnValue("keyPrefix", String.Empty)));
        }
    }
}
