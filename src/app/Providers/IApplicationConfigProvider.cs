using automation.components.data.v1.Config;

namespace automation.components.data.v1.Providers
{
    public interface IApplicationConfigProvider
    {
        bool SetApplicationConfig(ApplicationConfig config);

        ApplicationConfig GetApplicationConfig(string name, string _namespace, string environment);

        ApplicationConfig GetApplicationConfigByName(string name);
    }
}
