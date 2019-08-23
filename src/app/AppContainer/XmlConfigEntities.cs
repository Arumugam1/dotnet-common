using automation.components.data.v1.AppContainer;
using System.Configuration;

namespace automation.components.data.v1
{
    public class CustomEntities : ConfigurationSection
    {
        [ConfigurationProperty("", IsDefaultCollection = true)]
        public CustomEntity CustomEntity
        {
            get
            {
                return this[""] as CustomEntity;
            }
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
            return (element as Element).providerFullName;
        }

        protected override ConfigurationElement CreateNewElement()
        {
            return new Element();
        }

    }

    public class Element : ConfigurationElement
    {
        private const string AttributeAssemblyPath = "assemblyPath";
        private const string AttributeFullName = "fullName";
        private const string AttributeProviderFullName = "providerFullName";
        private const string AttributeInterfaceFullName = "interfaceFullName";

        [ConfigurationProperty(AttributeAssemblyPath, IsRequired = true, IsKey = false)]
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

        [ConfigurationProperty(AttributeProviderFullName, IsRequired = true, IsKey = true)]
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

        public Entity ToEntity()
        {
            return new Entity
            {
                assemblyPath = this.assemblyPath,
                fullName = this.fullName,
                providerFullName = this.providerFullName,
                interfaceFullName = this.interfaceFullName
            };
        }
    }
}
