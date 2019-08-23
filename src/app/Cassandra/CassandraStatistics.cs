using System.Collections.Generic;


namespace automation.components.data.v1.Cassandra
{
    /// <summary>
    /// CassandraStatistics
    /// List based storage for Cassandra stat records. Once the structure reaches _maxSize
    /// of references the oldest records are removed to make space for new ones
    /// </summary>
    public sealed class CassandraStatistics : List<CassandraStatisticalRecord>
    {
        private int _maxSize;
        private int _currentSize;

        public CassandraStatistics(int maximumSize) : base()
        {
            _maxSize = maximumSize;
        }

        public CassandraStatistics() : this(5242880)  // Default to 5MiB
        {
        }

        public int currentSize { get { return _currentSize; } }
        public int maximumSize { get { return _maxSize; } }

        private void FreeRefrences(int numberOfBytes)
        {
            int idx;
            int bytesToFree = 0;

            for (idx = 0; idx < this.Count; idx++)
            {
                
                if (bytesToFree >= numberOfBytes)
                    break;

                bytesToFree += this[idx].TotalSize;
            }
            _currentSize -= bytesToFree;
            this.RemoveRange(0, idx);
        }

        public new void Add(CassandraStatisticalRecord record)
        {
            int footprint = record.TotalSize;

            if(footprint + _currentSize > _maxSize)
            {
                FreeRefrences(footprint);
            }
            _currentSize += footprint;
            base.Add(record);
        }
        
        public void Add(CassandraStatisticalRecord record, long startTime)
        {
            record.StartTime = startTime;
            this.Add(record);
        }
    }

}
