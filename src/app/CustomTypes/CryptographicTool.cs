using FluentCassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace automation.components.data.v1.CustomTypes
{
    public class CryptographicTool
    {        
        public static Guid GetTimeBasedGuid(DateTime dt)
        {
            var generator = RandomNumberGenerator.Create();
            var p1 = new byte[2];
            generator.GetBytes(p1);
            var p2 = new byte[6];
            generator.GetBytes(p2);

            return GuidGenerator.GenerateTimeBasedGuid(dt, p1, p2);
        }
    }
}