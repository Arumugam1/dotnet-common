using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace automation.components.data.v1.CustomTypes
{
    public class PreProcessorException : System.Exception
    {
        public string Status;
        public string Description;
        public DateTime? ExpectedDequeueTime;

        public PreProcessorException()
        {
        }

        public PreProcessorException(string message)
            : base(message)
        {
        }

        public PreProcessorException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}
