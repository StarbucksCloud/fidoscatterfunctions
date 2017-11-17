using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Linq;
using System.Data.SqlClient;
using System.Data;
using System;
using FidoScatterGatherFunctions;

namespace FunctionApp2
{
    public static class Function4
    {
        private static readonly string DataTypeName = "labor";
        [FunctionName("Function4")]
        public static void Run([ServiceBusTrigger("fidoscattersearch", "laborScatterSubscription", AccessRights.Listen, Connection = "fidoScatterConnection")]string mySbMsg, TraceWriter log)
        {
            JObject joResponse = JObject.Parse(mySbMsg);
            //string storeid1 = joResponse.storeId;
           
            JArray ds = (JArray)joResponse["dataSources"];
            
            //string requestname = joResponse.name;
            //DateTime requestdate = joResponse.value;
            JObject parameters = (JObject)ds[1];
            DateTime requestdate = Convert.ToDateTime((string)parameters["value"]);



            //int id = Convert.ToInt32(array[0].toString());

            var request = JsonConvert.DeserializeObject<FidoScatterSearchRequest>(mySbMsg);
            var storeid1 = request?.StoreId.ToString();
            var myDataSource = request?.DataSources.First(p => DataTypeName.Equals(p.Name));
            
            if (myDataSource == null)
            {
                log.Info($"LaborGatherer Skipping {mySbMsg}");
                return;
            }

            try
            {
                //DataTable dt = new DataTable();
                using (SqlConnection con = new SqlConnection("Server=tcp:cljinventorysqlserv.database.windows.net,1433;Initial Catalog=cljpartnerschedule;Persist Security Info=False;User ID=cljadmind=Welcome123456789;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"))
                {
                    using (SqlCommand cmd = new SqlCommand("EXEC GetPartnerScheduleJson " + storeid1 + ", '" + requestdate + "'", con))
                    {
                        con.Open();
                        SqlDataAdapter da = new SqlDataAdapter(cmd);
                        //da.Fill(dt);
                        //int i = cmd.ExecuteNonQuery();
                        DataSet dset = new DataSet();
                        da.Fill(dset);

                        dset.WriteXml("c:\app.txt");




                    }
                }
            }
            catch (Exception ex)
            {
                log.Error(ex.Message);
            }

            log.Info($"C# ServiceBus topic trigger function processed message: {mySbMsg}");
        }
    }
}