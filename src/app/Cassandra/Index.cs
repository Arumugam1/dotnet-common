using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace automation.components.data.v1.Cassandra
{
    public class Index
    {
        public string Name { get; set; }
        public Dictionary<string, string> KeyValues { get; set; }
        public uint TimeToLive { get; set; }
    }
}
