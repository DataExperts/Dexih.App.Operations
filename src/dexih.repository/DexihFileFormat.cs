using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using dexih.functions.File;

namespace dexih.repository
{
	public partial class DexihFileFormat : DexihBaseEntity
    {
		public DexihFileFormat() => DexihTables = new HashSet<DexihTable>();

        [CopyCollectionKey((long)0, true)]
        public long FileFormatKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public bool IsDefault { get; set; }

        public bool MatchHeaderRecord { get; set; } = true;

        public bool AllowComments { get; set; } = false;
        public int BufferSize { get; set; } = 2048;

        [NotMapped]
        public char Comment { get; set; } = '#';

//        // Comment string is used, as EF databases do not support char values.
//        [JsonIgnore, CopyIgnore]
//        public string CommentString
//        {
//            get => Comment.ToString();
//            set => Comment = value?.ToCharArray(0, 1)[0] ?? '\0';
//        }

        public string Delimiter { get; set; } = ",";
        public bool DetectColumnCountChanges { get; set; } = false;
        public bool HasHeaderRecord { get; set; } = true;
        public bool IgnoreHeaderWhiteSpace { get; set; } = false;
        public bool IgnoreReadingExceptions { get; set; } = false;
        public bool IgnoreQuotes { get; set; } = false;

        [NotMapped]
        public char Quote { get; set; } = '\"';

        // Quote string is used, as EF databases do not support char values.
//        [JsonIgnore,CopyIgnore]
//        public string QuoteString
//        {
//            get => Quote.ToString();
//            set => Quote = value?.ToCharArray(0, 1)[0] ?? '\0';
//        }

        public bool QuoteAllFields { get; set; } = false;
        public bool QuoteNoFields { get; set; } = false;
        public bool SkipEmptyRecords { get; set; } = false;
        public bool TrimFields { get; set; } = false;
        public bool TrimHeaders { get; set; } = false;
        public bool WillThrowOnMissingField { get; set; } = true;

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihTable> DexihTables { get; set; }
        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }

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
