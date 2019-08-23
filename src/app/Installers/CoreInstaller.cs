using automation.components.container;
using automation.components.data.v1.Graph.Providers;
using automation.components.data.v1.Graph.Providers.Graphite;
using automation.components.data.v1.Providers;
using automation.components.data.v1.Providers.Cassandra;
using Castle.MicroKernel.Registration;
using Castle.MicroKernel.SubSystems.Configuration;
using Castle.Windsor;

namespace automation.components.data.v1.installers
{
    public class CoreInstaller : IWindsorInstaller, ICoreComponents
    {
        public void Install(IWindsorContainer container, IConfigurationStore store)
        {
            container.Register(Component
                .For<IApplicationConfigProvider>()
                .ImplementedBy<ApplicationConfigProvider>());

            container.Register(Component
                .For<IGraphProvider>()
                .ImplementedBy<GraphProvider>());
        }
    }
}
