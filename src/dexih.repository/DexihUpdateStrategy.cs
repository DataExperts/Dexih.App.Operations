//using Dexih.Utils.CopyProperties;
//using Newtonsoft.Json;
//using System;
//using System.Collections.Generic;
//using System.ComponentModel.DataAnnotations.Schema;
//using static dexih.transforms.TransformDelta;
//
//namespace dexih.repository
//{
//    public partial class DexihUpdateStrategy : DexihBaseEntity
//    {
//        public DexihUpdateStrategy()
//        {
//            DexihDatalinks = new HashSet<DexihDatalink>();
//        }
//
//        public long UpdateStrategyKey { get; set; }
//        public string Name { get; set; } = "Reload";
//        public string Description { get; set; }
//        public bool TruncateBeforeLoad { get; set; } = false;
//        public bool UpdateWhenExists { get; set; } = false;
//        public bool DeleteWhenNotExists { get; set; } = false;
//        public bool PreserveHistory { get; set; } = false;
//
//        [NotMapped]
//        [JsonIgnore, CopyIgnore]
//        public EUpdateStrategy Strategy => (EUpdateStrategy)Enum.Parse(typeof(EUpdateStrategy), Name);
//
//        /// <summary>
//        /// Indicates whether the update strategy requires the data to be presorted by the tables natural key.
//        /// </summary>
//        /// <returns></returns>
//        [NotMapped]
//        public bool RequiresSort
//        {
//            get
//            {
//                if (TruncateBeforeLoad == false && (UpdateWhenExists || DeleteWhenNotExists || PreserveHistory))
//                    return true;
//                else
//                    return false;
//            }
//        }
//
//        [JsonIgnore, CopyIgnore]
//        public virtual ICollection<DexihDatalink> DexihDatalinks { get; set; }
//    }
//}
