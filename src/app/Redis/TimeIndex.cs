using System;
using System.Collections.Generic;

namespace automation.components.data.v1.Redis
{
    public class TimeIndex
    {
        public string Name { get; set; }

        public Dictionary<DateTime, string> KeyValues { get; set; }

        public uint TimeToLive { get; set; }
    }
}