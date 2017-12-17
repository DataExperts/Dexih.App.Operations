using System.Collections.Generic;
using dexih.functions;
using dexih.repository;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.operations
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EImportAction
    {
        Replace, New, Leave, Skip
    }

    public class Import
    {
        public long HubKey { get; set; }
        public ImportObjects<DexihDatajob> Datajobs { get; set; }
        public ImportObjects<DexihDatalink> Datalinks { get; set; }
        public ImportObjects<DexihConnection> Connections { get; set; }
        public ImportObjects<DexihTable> Tables { get; set; }
        public ImportObjects<DexihColumnValidation> ColumnValidations { get; set; }
        public ImportObjects<DexihFileFormat> FileFormats { get; set; }

        public Import(long hubKey)
        {
            HubKey = hubKey;
            
            Datajobs = new ImportObjects<DexihDatajob>();
            Datalinks = new ImportObjects<DexihDatalink>();
            Connections = new ImportObjects<DexihConnection>();
            Tables = new ImportObjects<DexihTable>();
            ColumnValidations = new ImportObjects<DexihColumnValidation>();
            FileFormats = new ImportObjects<DexihFileFormat>();
        }
    }

    public class ImportObjects<T> : List<ImportObject<T>>
    {
        public void Add(T item, EImportAction importAction)
        {
            base.Add(new ImportObject<T>(item, importAction));            
        }
    }

    public class ImportObject<T>
    {
        public ImportObject(T item, EImportAction importAction)
        {
            Item = item;
            ImportAction = importAction;
        }
        
        public T Item { get; set; }   
        public EImportAction ImportAction { get; set; }
    }
    
    
}