using System.Collections.Generic;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;
using dexih.functions.File;

namespace dexih.repository
{
	public partial class DexihFileFormat : DexihHubBaseEntity
    {
		public DexihFileFormat() => DexihTables = new HashSet<DexihTable>();

        [CopyCollectionKey((long)0, true)]
        public long FileFormatKey { get; set; }

        public string Name { get; set; }
        public string Description { get; set; }
        public bool IsDefault { get; set; }

        public bool MatchHeaderRecord { get; set; } = true;
        public int SkipHeaderRows { get; set; } = 0;

        public bool AllowComments { get; set; } = false;
        public int BufferSize { get; set; } = 2048;

        public char Comment { get; set; } = '#';

        public string Delimiter { get; set; } = ",";
        public bool DetectColumnCountChanges { get; set; } = false;
        public bool HasHeaderRecord { get; set; } = true;
        public bool IgnoreHeaderWhiteSpace { get; set; } = false;
        public bool IgnoreReadingExceptions { get; set; } = false;
        public bool IgnoreQuotes { get; set; } = false;

        public char Quote { get; set; } = '\"';

        public bool QuoteAllFields { get; set; } = false;
        public bool QuoteNoFields { get; set; } = false;
        public bool SkipEmptyRecords { get; set; } = false;
        public bool TrimFields { get; set; } = false;
        public bool TrimHeaders { get; set; } = false;
        public bool WillThrowOnMissingField { get; set; } = true;
        public bool SetWhiteSpaceCellsToNull { get; set; } = true;

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihTable> DexihTables { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihHub Hub { get; set; }

        /// <summary>
        /// Converts to a FileFormat class.
        /// </summary>
        /// <returns></returns>
        public FileConfiguration GetFileFormat()
        {
            var fileFormat = new FileConfiguration();
            this.CopyProperties(fileFormat, false);
            return fileFormat;
        }
    }
}
