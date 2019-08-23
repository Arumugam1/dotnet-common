using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace automation.core.components.data.v1.Cassandra
{
    public class DataLayerException : Exception
    {
        public DataLayerException()
        {
        }

        public DataLayerException(string message) 
            : base(message)
        {
        }
        public DataLayerException(string message, Exception inner) 
            : base(message, inner)
        {
        }

    }

    public class DataLayerExceptionHandler
    {
        private string _className;

        public DataLayerExceptionHandler(string className)
        {
            Trace.Indent();
            Trace.WriteLine(String.Format("Exception handler initialized with {0}", className));
            _className = className;
        }

        public void HandleException(Exception ex, CassandraStatistics cs)
        {

        }

        public void HardLog()
        {

        }
    }
}
