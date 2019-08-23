using System;

namespace automation.components.data.v1.AppContainer
{
    public class ContainerConfigException : Exception
    {
        public ContainerConfigException(string message)
            : base(message)
        {
        }
    }
}
