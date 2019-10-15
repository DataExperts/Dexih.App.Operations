using dexih.functions.Query;
using dexih.repository;
using dexih.transforms;
using MessagePack;
using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.operations
{
    [MessagePackObject]
    public class DownloadData
    {
        [Key(0)]
        public TransformSettings TransformSettings { get; set; }

        [Key(1)]
        public CacheManager Cache { get; set; }

        [Key(2)]
        public DownloadObject[] DownloadObjects { get; set; }

        [Key(3)]
        public EDownloadFormat DownloadFormat { get; set; }

        [Key(4)]
        public bool ZipFiles { get; set; }

        public DownloadData() { }

        public DownloadData(TransformSettings transformSettings, CacheManager cache, DownloadObject[] downloadObjects, EDownloadFormat downloadFormat, bool zipFiles)
        {
            TransformSettings = transformSettings;
            Cache = cache;
            DownloadObjects = downloadObjects;
            DownloadFormat = downloadFormat;
            ZipFiles = zipFiles;
        }

        public async Task<(string FileName, Stream Stream)> GetStream(CancellationToken cancellationToken)
        {
            try
            {
                var transformManager = new TransformsManager(TransformSettings);

                var zipStream = new MemoryStream();
                var archive = new ZipArchive(zipStream, ZipArchiveMode.Create, true);

                foreach (var downloadObject in DownloadObjects)
                {
                    Transform transform = null;
                    var name = "";

                    if (downloadObject.ObjectType == EDataObjectType.Table)
                    {
                        var dbTable = Cache.Hub.DexihTables.SingleOrDefault(c => c.Key == downloadObject.ObjectKey);

                        if (dbTable != null)
                        {
                            var dbConnection = Cache.Hub.DexihConnections.SingleOrDefault( c => c.Key == dbTable.ConnectionKey);
                            var connection = dbConnection.GetConnection(TransformSettings);
                            var table = dbTable.GetTable(Cache.Hub, connection, downloadObject.InputColumns,
                                TransformSettings);
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
                            transform = new TransformQuery(transform, downloadObject.Query) {Name = "Stream Query"} ;
                            var openResult = await transform.Open(0, null, cancellationToken);
                            if (!openResult)
                            {
                                throw new DownloadDataException(
                                    $"The connection {connection.Name} with table {table.Name} failed to open for reading.");
                            }

                            transform.SetEncryptionMethod(EEncryptionMethod.EncryptDecryptSecureFields,
                                Cache.CacheEncryptionKey);
                        }
                    }
                    else
                    {
                        var dbDatalink = Cache.Hub.DexihDatalinks.SingleOrDefault(c => c.Key == downloadObject.ObjectKey);

                        if (dbDatalink == null)
                        {
                            throw new DownloadDataException($"The datalink with key {downloadObject.ObjectKey} could not be found in the cache.");
                        }

                        var transformWriterOptions = new TransformWriterOptions()
                        {
                            PreviewMode = true
                        };
                        
                        //Get the last Transform that will load the target table.
                        var runPlan = transformManager.CreateRunPlan(Cache.Hub, dbDatalink, downloadObject.InputColumns, downloadObject.DatalinkTransformKey, null, transformWriterOptions);
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

                    switch (DownloadFormat)
                    {
                        case EDownloadFormat.Csv:
                            name = name + ".csv";
                            fileStream = new StreamCsv(transform);
                            if (!ZipFiles)
                            {
                                return (name, fileStream);
                            }
                            break;
                        case EDownloadFormat.Json:
                            name = name + ".json";
                            fileStream = new StreamJson(transform);
                            if (!ZipFiles)
                            {
                                return (name, fileStream);
                            }
                            break;
                        case EDownloadFormat.JsonCompact:
                            name = name + ".json";
                            fileStream = new StreamJsonCompact(name, transform);
                            if (!ZipFiles)
                            {
                                return (name, fileStream);
                            }
                            break;
                            
                        default:
                            throw new Exception("The file format " + DownloadFormat + " is not currently supported for downloading data.");
                    }

                    var entry = archive.CreateEntry(name);
                    using (var entryStream = entry.Open())
                    {
                        await fileStream.CopyToAsync(entryStream, cancellationToken);
                    }
                }

                if (ZipFiles)
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

        [MessagePackObject]
        public class DownloadObject
        {
            [Key(0)]
            public EDataObjectType ObjectType { get; set; }

            [Key(1)]
            public long ObjectKey { get; set; }

            [Key(2)]
            public SelectQuery Query { get; set; }

            [Key(3)]
            public InputParameters InputParameters { get; set; }

            [Key(4)]
            public InputColumn[] InputColumns { get; set; }

            // used when downloading data from a specific a transform data.
            [Key(5)]
            public long? DatalinkTransformKey { get; set; }

        }

        public enum EDownloadFormat
        {
            Csv = 1, Json, JsonCompact
        }
    }
}
