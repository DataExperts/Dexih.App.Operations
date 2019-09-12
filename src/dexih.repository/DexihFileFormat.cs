using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;
using dexih.functions.File;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
	public partial class DexihFileFormat : DexihHubNamedEntity
    {
		public DexihFileFormat() => DexihTables = new HashSet<DexihTable>();

        [ProtoMember(1)]
        public bool IsDefault { get; set; }

        [ProtoMember(2)]
        public bool MatchHeaderRecord { get; set; } = true;

        [ProtoMember(3)]
        public int SkipHeaderRows { get; set; } = 0;

        [ProtoMember(4)]
        public bool AllowComments { get; set; } = false;

        [ProtoMember(5)]
        public int BufferSize { get; set; } = 2048;

        [ProtoMember(6)]
        public char Comment { get; set; } = '#';

        [ProtoMember(7)]
        public string Delimiter { get; set; } = ",";

        [ProtoMember(8)]
        public bool DetectColumnCountChanges { get; set; } = false;

        [ProtoMember(9)]
        public bool HasHeaderRecord { get; set; } = true;

        [ProtoMember(10)]
        public bool IgnoreHeaderWhiteSpace { get; set; } = false;

        [ProtoMember(11)]
        public bool IgnoreReadingExceptions { get; set; } = false;

        [ProtoMember(12)]
        public bool IgnoreQuotes { get; set; } = false;

        [ProtoMember(13)]
        public char Quote { get; set; } = '\"';

        [ProtoMember(14)]
        public bool QuoteAllFields { get; set; } = false;

        [ProtoMember(15)]
        public bool QuoteNoFields { get; set; } = false;

        [ProtoMember(16)]
        public bool SkipEmptyRecords { get; set; } = false;

        [ProtoMember(17)]
        public bool TrimFields { get; set; } = false;

        [ProtoMember(18)]
        public bool TrimHeaders { get; set; } = false;

        [ProtoMember(19)]
        public bool WillThrowOnMissingField { get; set; } = true;

        [ProtoMember(20)]
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
