using System;
using System.Collections.Generic;

namespace automation.components.data.v1.Cassandra
{
    public class TimeUUIDIndex
    {
        public string Name { get; set; }
        public Dictionary<Guid, string> KeyValues { get; set; }
        public uint TimeToLive { get; set; }
    }
}
