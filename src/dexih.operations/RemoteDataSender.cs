using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using dexih.functions;
using dexih.functions.Query;
using dexih.repository;
using Dexih.Utils.Crypto;
using Dexih.Utils.MessageHelpers;

namespace dexih.operations
{
    /// <summary>
    /// RemoteDataSender is used to send chunks of data to the "Reader" controller on the web server,
    /// which is then consumed by the Dexih.Reader connection
    /// </summary>
    public class RemoteDataSender
    {
        private const int RowsPerBuffer = 10000;

        private readonly HttpClient _httpClient;
        private readonly TransformSettings _transformSettings;
        private readonly string _url;
        
        public RemoteDataSender(TransformSettings transformSettings, HttpClient httpClient, string url)
        {
            _transformSettings = transformSettings;
            _httpClient = httpClient;
            _url = url;
        }

        public async Task SendDatalinkData(DexihHub hub, DexihDatalink datalink, ICollection<DexihColumnBase> inputColumns, GlobalVariables globalVariables, SelectQuery selectQuery, BufferBlock<object> buffer, CancellationToken cancellationToken)
        {
            try
            {
                var transformOperations = new TransformsManager(_transformSettings);
                var runPlan = transformOperations.CreateRunPlan(hub, datalink, inputColumns, globalVariables, null, null, false, selectQuery);
                var transform = runPlan.sourceTransform;

                var targetTable = datalink.GetOutputTable();
                
                var openReturn = await transform.Open(0, selectQuery, cancellationToken);
                if (!openReturn)
                {
                    throw new RemoteDataSenderException("Failed to open the transform.");
                }
                
                var columnCount = targetTable.DexihDatalinkColumns.Count;
                
                // create a ordinals array which is used to quickly map column names to the positions in the reader.
                var ordinals = new int[columnCount];
                var columns = targetTable.DexihDatalinkColumns.Select(c => c.Name).ToArray();
                for (var i = 0; i < columnCount; i++)
                {
                    ordinals[i] = transform.GetOrdinal(columns[i]);
                }
                
                var totalCount = 0;
                var bufferCount = 0;
                var dataSet = new object[RowsPerBuffer][];
                //loop through the transform to cache the preview data.
                while ((await transform.ReadAsync(cancellationToken)) && (selectQuery == null || totalCount < selectQuery.Rows || selectQuery.Rows == -1) &&
                       cancellationToken.IsCancellationRequested == false)
                {
                    var row = new object[columnCount];
                    
                    for (var i = 0; i < columnCount; i++)
                    {
                        row[i] = i < 0 ? null : transform[ordinals[i]];
                    }

                    dataSet[bufferCount] = row;
                    bufferCount++;
                    totalCount++;

                    if (bufferCount >= RowsPerBuffer)
                    {
                        var dataset = new DataSet() {columns = columns, data = dataSet};
                        await buffer.SendAsync(dataset, cancellationToken);
                        bufferCount = 0;
                    }
                }

                // send any remaining data
                if (bufferCount > 0)
                {
                    Array.Resize(ref dataSet, bufferCount);
                    var dataset = new DataSet() {columns = columns, data = dataSet};
                    await buffer.SendAsync(dataset, cancellationToken);
                }

                buffer.Complete();
                
                transform.Dispose();
            }
            catch (Exception ex)
            {
                var message = Json.SerializeObject(new
                {
                    Exception = ex
                }, "");

                var content = new StringContent(message, Encoding.UTF8, "application/json");
                await _httpClient.PostAsync(_url + "Reader/SetError", content, cancellationToken);

                throw;
            }
        }
        
        private async Task SendRemoteData(string[] columns, IEnumerable data, string continuationToken, CancellationToken cancellationToken)
        {
            var message = Json.SerializeObject(new
            {
                ContinuationToken = continuationToken,
                Dataset = new DataSet() { columns = columns, data = data}
            }, "");
            
            var content = new StringContent(message, Encoding.UTF8, "application/json");
            
            var response = await _httpClient.PostAsync(_url + "Reader/SendData", content, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                throw new RemoteDataSenderException($"The data could not be sent to the http server.  Reason {response.ReasonPhrase}.");
            }
            var returnValue = Json.DeserializeObject<ReturnValue>(await response.Content.ReadAsStringAsync(), _transformSettings.RemoteSettings.AppSettings.EncryptionKey);
            if (!returnValue.Success)
            {
                throw new RemoteDataSenderException($"The data could not be sent to the http server.  {returnValue.Message}", returnValue.Exception);
            }
        }
        
        private async Task SendComplete(string continuationToken, CancellationToken cancellationToken)
        {
            var content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("ContinuationToken", continuationToken),
            });

            var response = await _httpClient.PostAsync(_url + "Reader/Complete", content, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                throw new RemoteDataSenderException($"The data could not be sent to the http server.  Reason {response.ReasonPhrase}.");
            }
            var returnValue = Json.DeserializeObject<ReturnValue>(await response.Content.ReadAsStringAsync(), _transformSettings.RemoteSettings.AppSettings.EncryptionKey);
            if (!returnValue.Success)
            {
                throw new RemoteDataSenderException($"The data could not be sent to the http server.  {returnValue.Message}", returnValue.Exception);
            }
        }

        public class DataSet
        {
            public string[] columns { get; set; }
            public IEnumerable data { get; set; }
        }
    }
}