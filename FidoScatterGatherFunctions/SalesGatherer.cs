using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System.Linq;
using System.Net;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Collections.Generic;
using System.IO;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace FidoScatterGatherFunctions
{
    public static class SalesGatherer
    {
        private static readonly string DataTypeName = "sales";
        [FunctionName("SalesGatherer")]
        public static void Run([ServiceBusTrigger("fidoscattersearch", "salesGatherSubscription", AccessRights.Listen, Connection = "fidoScatterConnection")]string mySbMsg, 
            TraceWriter log)
        {
            var request = JsonConvert.DeserializeObject<FidoScatterSearchRequest>(mySbMsg);
            var myDataSource = request?.DataSources.First(p => DataTypeName.Equals(p.Name));
            
            if (myDataSource == null)
            {
                log.Info($"SalesGatherer Skipping {mySbMsg}");
                return;
            }
            var data = FetchData(request.StoreId, myDataSource, log);

            WriteToBlob(data, request.CorrelationId);
            
   
            log.Info($"C# ServiceBus topic trigger function processed message: {mySbMsg}");
        }

        private static IList<Sale> FetchData(string storeId, FidoScatterSearchRequest.DataSource dataSource, TraceWriter log)
        {
            var endpoint =new System.Uri(System.Environment.GetEnvironmentVariable("storeDbEndpoint"));
            var key = System.Environment.GetEnvironmentVariable("storeDbKey");
            var databaseName = "salesdb";
            var collectionName = "salescollection";
            DocumentClient client = new DocumentClient(endpoint, key);

            FeedOptions queryOptions = new FeedOptions { MaxItemCount = -1 };

            IQueryable<Sale> sales = client.CreateDocumentQuery<Sale>(
                    UriFactory.CreateDocumentCollectionUri(databaseName, collectionName), queryOptions)
                   .Where(s => s.Location == int.Parse(storeId));

            var items = sales.ToList();


            return items;
        }

        private static void WriteToBlob<T>(T data, string correlationId) 
            where T:class
        {
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(System.Environment.GetEnvironmentVariable("SalesDataStorageConnectionAppSetting"));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("salesdata");

            // Retrieve reference to a blob named "myblob".
            CloudBlockBlob blockBlob = container.GetBlockBlobReference(correlationId);

            Stream dataStream = new MemoryStream();
            using (StreamWriter writer = new StreamWriter(dataStream))
            using (JsonTextWriter jsonWriter = new JsonTextWriter(writer))
            {
                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(jsonWriter, data);
                jsonWriter.Flush();
                // Create or overwrite the "myblob" blob with contents from a local file.
                dataStream.Position = 0;
                blockBlob.UploadFromStream(dataStream);
            }
        }
    }
    
    internal class Sale
    {
        public int Id { get; set; }
        public int Location { get; set; }
        public DateTime Date { get; set; }
        public DateTime TimeOfSale { get; set; }
        public string ItemNumber { get; set;}
        public string ItemName { get; set; }
        public int Quantity { get; set; }
        public decimal Price { get; set; }
        public decimal Value { get; set; }

        public override string ToString()
        {
            return $"{Location} {ItemNumber} {Quantity}";
        }
    }
}
