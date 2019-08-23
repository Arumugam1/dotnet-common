using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace automation.core.components.data.v1.Entities
{
    public enum CounterType
    {
        Decrement = 1,
        Increment = 2,
        Reset = 3
    }

    public class CounterIndex
    {
        public Dictionary<string, object> KeyValues { get; set; }
        public string Name { get; set; }
        public int Value { get; set; }
        public CounterType Type { get; set; }
    }
}
