using System;
using System.Data;
using automation.components.si.dal.v1.Operations;

namespace automation.components.data.v1.MSSQL
{
    public interface IDataExplorer<T>
    {
        DataSet ExecuteDataSet(Command cmd);
        object ExecuteScalar(Command cmd);
        int ExecuteNonQuery(Command cmd);
    }

    public abstract class DataExplorer<T> : IDataExplorer<T>, IDisposable
    {
        #region ICassandraCommunicator Members::

        public DataSet ExecuteDataSet(Command cmd)
        {
            return cmd.ExecuteDataSet();
        }

        public object ExecuteScalar(Command cmd)
        {
            return cmd.ExecuteScalar();
        }

        public int ExecuteNonQuery(Command cmd)
        {
            return cmd.ExecuteNonQuery();
        }

        #endregion

        #region IDisposable Members::

        public virtual void Dispose()
        {

        }

        #endregion
    }
}
