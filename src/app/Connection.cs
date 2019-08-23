using System;
using System.Configuration;
using System.Data.SqlClient;

namespace automation.components.data.v1.Connection
{
    public static class Manager
    {
        static SqlConnection SqlConnection;
        public static SqlConnection GetSQLConnection()
        {
            try
            {
                if (SqlConnection == null)
                {
                    SqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings["sqlConfig"].ConnectionString);
                }
            }
            catch (Exception ex)
            {
                automation.components.operations.v1.Trace.Exception(automation.components.operations.v1.TraceTypes.AccessLayer, "automation.components.data.v1.Connection.Manager", "GetSQLConnection", new object[] { }, ex);
            }

            if (SqlConnection == null && System.AppDomain.CurrentDomain.BaseDirectory.Contains("NUnit"))
            {
                SqlConnection = new SqlConnection("Data Source=si-stgdb.intensive.int;Initial Catalog=SI;Persist Security Info=True;User ID=pingwatch;Password=Ping;pooling=true;Max Pool Size=1000;Connect Timeout=60;");
            }


            return SqlConnection;
        }

        public static SqlConnection GetSQLConnection(string connectionStringKey)
        {
            SqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[connectionStringKey].ConnectionString);
            return SqlConnection;
        }
    }
}