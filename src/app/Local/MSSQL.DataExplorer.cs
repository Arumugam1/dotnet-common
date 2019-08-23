using System;
using System.Data;
using System.Linq;
using automation.components.data.v1.MSSQL;
using automation.components.si.dal.v1.Operations;

namespace automation.components.data.v1.Local.MSSQL
{
    public class DataExplorer<T> : IDataExplorer<T>, IDisposable
    {
        public DataSet ExecuteDataSet(Command cmd)
        {
            if (!LocalContainer.sqlData.Tables.Contains(cmd.CommandText))
                return new DataSet();

            var ds = new DataSet();

            if (LocalContainer.sqlData.Tables[cmd.CommandText].Rows != null && LocalContainer.sqlData.Tables[cmd.CommandText].Rows.Count > 0)
            {
                var dt =
                    LocalContainer.sqlData.Tables[cmd.CommandText].Rows.Cast<DataRow>().Where(
                        row => !row.ItemArray.All(field => ReferenceEquals(field, DBNull.Value) | field.Equals(""))).
                        CopyToDataTable();

                ds.Tables.Add(dt);
            }

            return ds;
        }

        public object ExecuteScalar(Command cmd)
        {
            if (!LocalContainer.sqlData.Tables.Contains(cmd.CommandText))
                return new object();

            return LocalContainer.sqlData.Tables[cmd.CommandText].Columns[0];
        }

        public int ExecuteNonQuery(Command cmd)
        {
            return 0;
        }

        public void Dispose()
        {
            LocalContainer.data = null;
        }
    }
}
