using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using CsvHelper.Configuration;
using dexih.transforms.File;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [DataContract]
	public class DexihFileFormat : DexihHubNamedEntity
    {
		public DexihFileFormat() => DexihTables = new HashSet<DexihTable>();

        [DataMember(Order = 7)]
        public bool IsDefault { get; set; }

        [DataMember(Order = 8)]
        public bool MatchHeaderRecord { get; set; } = true;

        [DataMember(Order = 9)]
        public int SkipHeaderRows { get; set; } = 0;

        [DataMember(Order = 10)]
        public bool AllowComments { get; set; } = false;

        [DataMember(Order = 11)]
        public int BufferSize { get; set; } = 2048;

        [DataMember(Order = 12)]
        [DefaultValue(35)]
        public char Comment { get; set; } = '#';

        [DataMember(Order = 13)]
        public string Delimiter { get; set; } = ",";

        [DataMember(Order = 14)]
        public bool DetectColumnCountChanges { get; set; } = false;

        [DataMember(Order = 15)]
        public bool HasHeaderRecord { get; set; } = true;

        [DataMember(Order = 16)]
        public bool IgnoreHeaderWhiteSpace { get; set; } = false;

        [DataMember(Order = 17)]
        public bool IgnoreReadingExceptions { get; set; } = false;

        [DataMember(Order = 18)]
        public bool IgnoreQuotes { get; set; } = false;

        [DataMember(Order = 19)]
        [DefaultValue(34)]
        public char Quote { get; set; } = '\"';

        [DataMember(Order = 20)]
        public bool QuoteAllFields { get; set; } = false;

        [DataMember(Order = 21)]
        public bool QuoteNoFields { get; set; } = false;

        [DataMember(Order = 22)]
        public bool SkipEmptyRecords { get; set; } = false;

        [DataMember(Order = 23)]
        public bool TrimFields { get; set; } = false;

        [DataMember(Order = 24)]
        public bool TrimHeaders { get; set; } = false;

        [DataMember(Order = 25)]
        public bool WillThrowOnMissingField { get; set; } = true;

        [DataMember(Order = 26)]
        public bool SetWhiteSpaceCellsToNull { get; set; } = true;

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
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
            var fileFormat = new FileConfiguration()
            {
                Comment = Comment,
                Delimiter = Delimiter,
                Quote = Quote,
                AllowComments = AllowComments,
                BufferSize = BufferSize,
                IgnoreQuotes = IgnoreQuotes,
                HasHeaderRecord = HasHeaderRecord,
                MatchHeaderRecord = MatchHeaderRecord,
                SkipHeaderRows = SkipHeaderRows,
                IgnoreBlankLines = SkipEmptyRecords,
                DetectColumnCountChanges = DetectColumnCountChanges,
                SetWhiteSpaceCellsToNull = SetWhiteSpaceCellsToNull,
                TrimOptions = TrimFields ? TrimOptions.Trim : TrimOptions.None,
            };

            if (IgnoreHeaderWhiteSpace || TrimHeaders)
            {
                fileFormat.PrepareHeaderForMatch = (header, i) =>
                {
                    if (string.IsNullOrEmpty(header))
                    {
                        return null;
                    }
                    string value = header;
                    if (IgnoreHeaderWhiteSpace)
                    {
                        value = Regex.Replace(value, @"\s", string.Empty);
                    }

                    if (TrimHeaders)
                    {
                        value = value.Trim();
                    }
                    return Regex.Replace(header, @"\s", string.Empty);
                };
            }

            if (IgnoreReadingExceptions)
            {
                fileFormat.ReadingExceptionOccurred = exception => { return true; };
            }

            if (QuoteAllFields)
            {
                fileFormat.ShouldQuote = (s, context) => true;
            }

            if (QuoteNoFields)
            {
                fileFormat.ShouldQuote = (s, context) => false;
            }

            if (WillThrowOnMissingField)
            {
                fileFormat.MissingFieldFound = (strings, i, arg3) =>
                {
                    throw new RepositoryException($"The field {arg3.Field} was missing");
                };
            }

            this.CopyProperties(fileFormat, false);

            if (TrimFields)
            {
                fileFormat.TrimOptions = TrimOptions.Trim;
            }

            return fileFormat;
        }
    }
}
