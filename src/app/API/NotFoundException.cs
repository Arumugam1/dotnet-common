
namespace automation.components.data.v1.API
{
    /// <summary>
    /// Represents error that occurs when a resource is not available. 
    /// </summary>
    public class NotFoundException : System.Exception
    {
        public NotFoundException()
            : base()
        {

        }

        public NotFoundException(string message)
            : base(message)
        {

        }

        public NotFoundException(string message, System.Exception innerException)
            : base(message, innerException)
        {

        }
    }
}
