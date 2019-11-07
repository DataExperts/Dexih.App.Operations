using System.Collections.Generic;
using System.ComponentModel;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using dexih.functions.File;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
	public partial class DexihFileFormat : DexihHubNamedEntity
    {
		public DexihFileFormat() => DexihTables = new HashSet<DexihTable>();

        [Key(7)]
        public bool IsDefault { get; set; }

        [Key(8)]
        public bool MatchHeaderRecord { get; set; } = true;

        [Key(9)]
        public int SkipHeaderRows { get; set; } = 0;

        [Key(10)]
        public bool AllowComments { get; set; } = false;

        [Key(11)]
        public int BufferSize { get; set; } = 2048;

        [Key(12)]
        [DefaultValue(35)]
        public char Comment { get; set; } = '#';

        [Key(13)]
        public string Delimiter { get; set; } = ",";

        [Key(14)]
        public bool DetectColumnCountChanges { get; set; } = false;

        [Key(15)]
        public bool HasHeaderRecord { get; set; } = true;

        [Key(16)]
        public bool IgnoreHeaderWhiteSpace { get; set; } = false;

        [Key(17)]
        public bool IgnoreReadingExceptions { get; set; } = false;

        [Key(18)]
        public bool IgnoreQuotes { get; set; } = false;

        [Key(19)]
        [DefaultValue(34)]
        public char Quote { get; set; } = '\"';

        [Key(20)]
        public bool QuoteAllFields { get; set; } = false;

        [Key(21)]
        public bool QuoteNoFields { get; set; } = false;

        [Key(22)]
        public bool SkipEmptyRecords { get; set; } = false;

        [Key(23)]
        public bool TrimFields { get; set; } = false;

        [Key(24)]
        public bool TrimHeaders { get; set; } = false;

        [Key(25)]
        public bool WillThrowOnMissingField { get; set; } = true;

        [Key(26)]
        public bool SetWhiteSpaceCellsToNull { get; set; } = true;

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public ICollection<DexihTable> DexihTables { get; set; }
        
        public override void ResetKeys()
        {
            Key = 0;
        }
        
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
