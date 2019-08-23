using System;

namespace automation.components.data.v1.API
{
    [Serializable()]
    public class Exception
    {
        public String Error { get; set; }
        public String StackTrace { get; set; }
        public Exception InnerException { get; set; }
        public string SystemInfo { get; set; }

        public Exception()
        {
        }

        public Exception(System.Exception ex)
        {
            if (ex == null)
                return;

            Error = ex.Message;
            StackTrace = ex.StackTrace;
            InnerException = new Exception(ex.InnerException);
            SystemInfo = System.Environment.MachineName;
        }
    }
}
