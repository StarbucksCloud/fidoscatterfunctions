using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System.Linq;

namespace FidoScatterGatherFunctions
{
    public static class InventoryGatherer
    {
        private static readonly string DataTypeName = "inventory";
        [FunctionName("InventoryGatherer")]
        public static void Run([ServiceBusTrigger("fidoscattersearch", "inventoryScatterSubscription", AccessRights.Listen, Connection = "fidoScatterConnection")]string mySbMsg, TraceWriter log)
        {
            var request = JsonConvert.DeserializeObject<FidoScatterSearchRequest>(mySbMsg);
            var myDataSource = request?.DataSources.First(p => DataTypeName.Equals(p.Name));
            if (myDataSource == null)
            {
                log.Info($"InventoryGatherer Skipping {mySbMsg}");
                return;
            }
            log.Info($"C# ServiceBus topic trigger function processed message: {mySbMsg}");
        }
    }
}
