using System.Collections.Generic;

namespace automation.components.data.v1.Redis
{
    public class Index
    {
        public string Name { get; set; }

        public Dictionary<string, string> KeyValues { get; set; }

        public uint TimeToLive { get; set; }
    }
}