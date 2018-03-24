using dexih.functions.Query;
using dexih.repository;
using dexih.transforms;
using System;
using System.Collections.Generic;
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
                        foreach (var dbConnection in cache.DexihHub.DexihConnections)
                        {
                            var dbTable = dbConnection.DexihTables.SingleOrDefault(c => c.TableKey == downloadObject.ObjectKey);
                            if (dbTable != null)
                            {
                                var connection = dbConnection.GetConnection(_transformSettings);
                                var table = dbTable.GetTable(connection, _transformSettings);
                                name = table.Name + ".csv";

                                transform = connection.GetTransformReader(table, true);
                                var openResult = await transform.Open(0, downloadObject.Query, cancellationToken);
                                if (!openResult)
                                {
                                    throw new DownloadDataException($"The connection {connection.Name} with table {table.Name} failed to open for reading.");
                                }
                            }
                        }
                    }
                    else
                    {
                        var dbDatalink = cache.DexihHub.DexihDatalinks.SingleOrDefault(c => c.DatalinkKey == downloadObject.ObjectKey);
                        //Get the last Transform that will load the target table.
                        var runPlan = transformManager.CreateRunPlan(cache.DexihHub, dbDatalink, null, null, false);
                        transform = runPlan.sourceTransform;
                        var openReturn = await transform.Open(0, null, cancellationToken);
                        if (!openReturn)
                        {
                            throw new DownloadDataException($"The datalink {dbDatalink.Name} failed to open for reading.");
                        }

                        transform.SetCacheMethod(transforms.Transform.ECacheMethod.OnDemandCache);
                        transform.SetEncryptionMethod(EEncryptionMethod.MaskSecureFields, "");

                        name = dbDatalink.Name + ".csv";
                    }

                    Stream fileStream = null;

                    switch (downloadFormat)
                    {
                        case EDownloadFormat.Csv:
                            fileStream = new TransformCsvStream(transform);
                            if (!zipFiles)
                            {
                                return (name, fileStream);
                            }
                            break;
                        case EDownloadFormat.Json:
                            fileStream = new TransformJsonStream(transform);
                            if (!zipFiles)
                            {
                                return (name, fileStream);
                            }
                            break;
                            
                        default:
                            throw new Exception("The file format " + downloadFormat.ToString() + " is not currently supported for downloading data.");
                    }

                    if (zipFiles)
                    {
                        var entry = archive.CreateEntry(name);
                        using (var entryStream = entry.Open())
                        {
                            fileStream.CopyTo(entryStream);
                        }
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
        }

        public enum EDownloadFormat
        {
            Csv, Excel, Pdf, Raw, Json
        }
    }
}
