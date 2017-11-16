using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FidoScatterGatherFunctions
{
    internal class FidoScatterSearchRequest
    {
        public string CorrelationId { get; set; }
        public string StoreId { get; set; }
        public IEnumerable<DataSource> DataSources { get; set; }

        public class DataSource
        {
            public string Name { get; set; }
            public IEnumerable<Parameter> Parameters {get;set;}

            public class Parameter
            {
                public string Key { get; set; }
                public string Operator { get; set; }
                public string Value { get; set; }
            }
        }
    }

    
}
