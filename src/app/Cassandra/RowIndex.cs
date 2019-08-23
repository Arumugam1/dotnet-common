using System.Collections.Generic;

namespace automation.core.components.data.v1.Cassandra
{
    public class RowIndex
    {
        public Dictionary<string, object> KeyValues { get; set; }
        public uint TimeToLive { get; set; }
        public bool EnableAutoTimeUUID { get; set; }
        /// <summary>
        /// Set to true : it appends entry_timestamp column to query with now() as value
        /// Cassandra will generate timestamp if now() is set
        /// </summary>
        public bool EnableAutoCreationTimeUUID { get; set; }
    }
}
