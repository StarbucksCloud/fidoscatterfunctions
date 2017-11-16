using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System.Linq;

namespace FidoScatterGatherFunctions
{
    public static class LaborGatherer
    {
        private static readonly string DataTypeName = "labor";
        [FunctionName("LaborGatherer")]
        public static void Run([ServiceBusTrigger("fidoscattersearch", "laborScatterSubscription", AccessRights.Listen, Connection = "fidoScatterConnection")]string mySbMsg, TraceWriter log)
        {

            var request = JsonConvert.DeserializeObject<FidoScatterSearchRequest>(mySbMsg);
            var myDataSource = request?.DataSources.First(p => DataTypeName.Equals(p.Name));
            if (myDataSource == null)
            {
                log.Info($"LaborGatherer Skipping {mySbMsg}");
                return;
            }
            log.Info($"C# ServiceBus topic trigger function processed message: {mySbMsg}");
        }
    }
}
