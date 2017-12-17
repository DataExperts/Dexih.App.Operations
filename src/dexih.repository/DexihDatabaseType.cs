using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using static dexih.transforms.Connection;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihDatabaseType : DexihBaseEntity
    {
        //[JsonConverter(typeof(StringEnumConverter))]
        //public enum EDatabaseTypeCategory
        //{
        //    Database,
        //    WebService,
        //    File
        //}
        public DexihDatabaseType()
        {
            DexihConnections = new HashSet<DexihConnection>();
        }

        public long DatabaseTypeKey { get; set; }

        [NotMapped]
        public ECategory Category { get; set; }
        [JsonIgnore, CopyIgnore]
        public string CategoryString
        {
            get { return Category.ToString(); }
            set { Category = (ECategory)Enum.Parse(typeof(ECategory), value); }
        }
        public bool AllowSource { get; set; }
        public bool AllowManaged { get; set; }
        public bool AllowTarget { get; set; }
        public string Name { get; set; }
        public string Class { get; set; }
        public string Assembly { get; set; }
        public bool AllowUserpass { get; set; }
        public bool AllowWindowsAuth { get; set; }
        public bool AllowDatabase { get; set; }
        public bool AllowConnectionstring { get; set; }
        public string DatabaseHelp { get; set; }
        public string ServerHelp { get; set; }

        [NotMapped]
        public string UnsupportedCharacters { get; set; } = " ";

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihConnection> DexihConnections { get; set; }

        public string RemoveUnsupportedCharacaters(string name)
        {
            foreach (var r in UnsupportedCharacters)
            {
                name = name.Replace(r.ToString(), "");
            }

            return name;
        }
    }
}
