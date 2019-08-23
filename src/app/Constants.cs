using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace automation.components.data.v1
{
    internal static class Constants
    {
        #region Cassandra Constants
        public const string CassandraConnection = "cass.con";
        public const string CassandraConnectionFailed = "cass.confail";
        public const string CassLatency_GetIndex = "cass.lat.getindx";
        public const string CassLatency_GetIndexes = "cass.lat.getindxs";
        public const string CassLatency_AddIndexesWithTimeUUID = "cass.lat.addindxswtimeid";
        public const string CassLatency_AddIndexes = "cass.lat.addindxs";
        public const string CassLatency_GetUniqueIndex = "cass.lat.getunqindx";
        public const string CassLatency_GetIndexesColumns = "cass.lat.getindxclmns";
        public const string CassLatency_GetIndexColumnNames = "cass.lat.getindxclmnnames";
        public const string CassLatency_GetIndexColumnValues = "cass.lat.getindxclmnvals";
        public const string CassLatency_GetIndexesAsObject = "cass.lat.getindxasobj";
        public const string CassLatency_GetTimeIndex = "cass.lat.gettimeindx";
        public const string CassLatency_GetTimeIndexColumnValues = "cass.lat.gettimeindxclmnvals";
        public const string CassLatency_GetTimeIndexColumnNames = "cass.lat.gettimeindxclmnnames";
        public const string CassLatency_DeleteIndex = "cass.lat.delindx";
        public const string CassLatency_DeleteTimeIndex = "cass.lat.deltimeindx";
        public const string CassLatency_Count = "cass.lat.ctr";
        public const string CassLatency_ExecuteQuery = "cass.lat.exeq";
        public const string CassLatency_Update = "cass.lat.upd";
        public const string CassLatency_ExecuteNonQuery = "cass.lat.exenonq";
        public const string CassLatency_UpsertCounter = "cass.lat.upsertctr";
        public const string CassLatency_GetDataAsObject = "cass.lat.getdataasobj";
        public const string CassLatency_Add = "cass.lat.add";
        public const string CassLatency_Get = "cass.lat.get";
        public const string CassLatency_GetAsString = "cass.lat.getasstr";
        public const string CassLatency_GetDataAsString = "cass.lat.getdataasstr";
        public const string CassLatency_Filter = "cass.lat.filt";
        public const string CassLatency_GetAllRows = "cass.lat.getallrw";
        public const string CassLatency_Delete = "cass.lat.del";
        #endregion

        #region Redis Constants
        public const string RedisConnectionFailed = "red.confail";
        public const string RedisConnectionFailedOtherType = "red.confailot";
        public const string RedisException = "red.ex";
        public const string RedLatency_Add = "red.lat.add";
        public const string RedLatency_Get = "red.lat.get";
        public const string RedLatency_GetAsString = "red.lat.getasstr";
        public const string RedLatency_Delete = "red.lat.del";
        public const string RedLatency_AddIndexes = "red.lat.addindxs";
        public const string RedLatency_GetIndex = "red.lat.getindx";
        public const string RedLatency_GetIndexColumnNames = "red.lat.getindxclmnnames";
        public const string RedLatency_GetIndexColumnValues = "red.lat.getindxclmnvals";
        public const string RedLatency_GetIndexValue = "red.lat.getindxval";
        public const string RedLatency_DeleteIndex = "red.lat.delindx";
        public const string RedLatency_Count = "red.lat.ctr";
        #endregion

    }
}
