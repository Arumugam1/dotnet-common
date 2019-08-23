using System;

namespace automation.components.data.v1.Cassandra
{
    public class CassandraStatisticalRecord
    {

        public CassandraStatisticalRecord()
        {
            DataLength = 0;
            BatchWithError = false;
        }

        public CassandraStatisticalRecord(string query) : this()
        {
            this.Query = query;
        }

        public long StartTime { get; set; }
        public long EndTime { get; set; }
        public string Query { get; set; }

        // Sizes are in bytes
        public int QueryLength { get { return this.Query != null ? GetByteCount(Query) : 0; } }
        public int DataLength { get; set;}

        public bool BatchWithError { get; set; }

        public static int GetByteCount(string s)
        {
            return s.Length * sizeof(Char);
        }

        public int TotalSize
        {
            get
            {
                return QueryLength + DataLength;
            }
        }
    }
}
