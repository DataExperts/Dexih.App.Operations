using dexih.functions.Query;
using dexih.repository;
using dexih.transforms;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static dexih.transforms.Transform;

namespace dexih.operations
{
    public class DownloadData
    {
        private readonly TransformSettings _transformSettings;

        public DownloadData(TransformSettings transformSettings)
        {
            _transformSettings = transformSettings;
        }

        public async Task<(string FileName, Stream Stream)> GetStream(CacheManager cache, DownloadObject[] downloadObjects, EDownloadFormat downloadFormat, bool zipFiles, CancellationToken cancellationToken)
        {
            try
            {
                var transformManager = new TransformsManager(_transformSettings);

                var zipStream = new MemoryStream();
                var archive = new ZipArchive(zipStream, ZipArchiveMode.Create, true);

                foreach (var downloadObject in downloadObjects)
                {
                    Transform transform = null;
                    var name = "";

                    if (downloadObject.ObjectType == SharedData.EObjectType.Table)
                    {
                        var dbTable = cache.Hub.DexihTables.SingleOrDefault(c => c.Key == downloadObject.ObjectKey);

                        if (dbTable != null)
                        {
                            var dbConnection = cache.Hub.DexihConnections.SingleOrDefault( c => c.Key == dbTable.ConnectionKey);
                            var connection = dbConnection.GetConnection(_transformSettings);
                            var table = dbTable.GetTable(cache.Hub, connection, downloadObject.InputColumns,
                                _transformSettings);
                            name = table.Name;

                            if (downloadObject.InputColumns != null)
                            {
                                foreach (var inputColumn in downloadObject.InputColumns)
                                {
                                    var column = table.Columns.SingleOrDefault(c => c.Name == inputColumn.Name);
                                    if (column != null)
                                    {
                                        column.DefaultValue = inputColumn.Value;
                                    }
                                }
                            }

                            transform = connection.GetTransformReader(table, true);
                            transform = new TransformQuery(transform, downloadObject.Query);
                            var openResult = await transform.Open(0, null, cancellationToken);
                            if (!openResult)
                            {
                                throw new DownloadDataException(
                                    $"The connection {connection.Name} with table {table.Name} failed to open for reading.");
                            }

                            transform.SetEncryptionMethod(EEncryptionMethod.EncryptDecryptSecureFields,
                                cache.CacheEncryptionKey);
                        }
                    }
                    else
                    {
                        var dbDatalink = cache.Hub.DexihDatalinks.SingleOrDefault(c => c.Key == downloadObject.ObjectKey);

                        if (dbDatalink == null)
                        {
                            throw new DownloadDataException($"The datalink with key {downloadObject.ObjectKey} could not be found in the cache.");
                        }

                        var transformWriterOptions = new TransformWriterOptions()
                        {
                            PreviewMode = true
                        };
                        
                        //Get the last Transform that will load the target table.
                        var runPlan = transformManager.CreateRunPlan(cache.Hub, dbDatalink, downloadObject.InputColumns, null, downloadObject.DatalinkTransformKey, transformWriterOptions);
                        transform = runPlan.sourceTransform;
                        var openReturn = await transform.Open(0, null, cancellationToken);
                        if (!openReturn)
                        {
                            throw new DownloadDataException($"The datalink {dbDatalink.Name} failed to open for reading.");
                        }

                        transform.SetCacheMethod(ECacheMethod.DemandCache);
                        transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                        name = dbDatalink.Name;
                    }

                    Stream fileStream = null;

                    switch (downloadFormat)
                    {
                        case EDownloadFormat.Csv:
                            name = name + ".csv";
                            fileStream = new StreamCsv(transform);
                            if (!zipFiles)
                            {
                                return (name, fileStream);
                            }
                            break;
                        case EDownloadFormat.Json:
                            name = name + ".json";
                            fileStream = new StreamJson(name, transform);
                            if (!zipFiles)
                            {
                                return (name, fileStream);
                            }
                            break;
                        case EDownloadFormat.JsonCompact:
                            name = name + ".json";
                            fileStream = new StreamJsonCompact(name, transform);
                            if (!zipFiles)
                            {
                                return (name, fileStream);
                            }
                            break;
                            
                        default:
                            throw new Exception("The file format " + downloadFormat + " is not currently supported for downloading data.");
                    }

                    var entry = archive.CreateEntry(name);
                    using (var entryStream = entry.Open())
                    {
                        fileStream.CopyTo(entryStream);
                    }
                }

                if (zipFiles)
                {
                    archive.Dispose();
                    zipStream.Seek(0, SeekOrigin.Begin);
                    return ("datadownload.zip", zipStream);
                }

                throw new DownloadDataException("Unknown error.");
            }
            catch (Exception ex)
            {
                throw new DownloadDataException($"The download data failed.  {ex.Message}", ex);
            }
        }

        public class DownloadObject
        {
            public SharedData.EObjectType ObjectType { get; set; }
            public long ObjectKey { get; set; }
            public SelectQuery Query { get; set; }
            public InputColumn[] InputColumns { get; set; }

            // used when downloading data from a specific a transform data.
            public long? DatalinkTransformKey { get; set; }

        }

        public enum EDownloadFormat
        {
            Csv, Json, JsonCompact
        }
    }
}
