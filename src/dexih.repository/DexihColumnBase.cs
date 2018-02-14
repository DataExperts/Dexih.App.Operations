using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;
using System.ComponentModel.DataAnnotations.Schema;
using static dexih.functions.TableColumn;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    /// <summary>
    /// Base class for table columns.  Inherited by DexihTableColumn and DexihDatalinkColumn
    /// </summary>
    public class DexihColumnBase: DexihBaseEntity
    {
        public int Position { get; set; }

        public string Name { get; set; }

        public string LogicalName { get; set; }

        public string Description { get; set; }

        [JsonIgnore, CopyIgnore]
        public string DatatypeString
        {
            get => Datatype.ToString();
            set => Datatype = (ETypeCode)Enum.Parse(typeof(ETypeCode), value);
        }

        [NotMapped]
        public ETypeCode Datatype { get; set; }

        public int? MaxLength { get; set; }

        public int? Precision { get; set; }
        public bool? IsUnicode { get; set; }

        public int? Scale { get; set; }

        public bool AllowDbNull { get; set; }

        [JsonIgnore, CopyIgnore]
        public string DeltaTypeString
        {
            get => DeltaType.ToString();
            set => DeltaType = (EDeltaType)Enum.Parse(typeof(EDeltaType), value);
        }

        [NotMapped]
        [JsonConverter(typeof(StringEnumConverter))]
        public EDeltaType DeltaType { get; set; }

        public string DefaultValue { get; set; }
        public bool IsUnique { get; set; }

        public bool IsMandatory { get; set; }

        public bool IsIncrementalUpdate { get; set; }

        public bool IsInput { get; set; }

        [JsonIgnore, CopyIgnore]
        public string SecurityFlagString
        {
            get => SecurityFlag.ToString();
            set => SecurityFlag = (ESecurityFlag)Enum.Parse(typeof(ESecurityFlag), value);
        }

        [NotMapped]
        [JsonConverter(typeof(StringEnumConverter))]
        public ESecurityFlag SecurityFlag { get; set; }


        [NotMapped]
        /// <summary>
        /// Is the column one form the source (vs. a value added column).
        /// </summary>
        /// <returns></returns>
        public bool IsSourceColumn
        {
            get
            {
                switch (DeltaType)
                {
                    case EDeltaType.NaturalKey:
                    case EDeltaType.TrackingField:
                    case EDeltaType.NonTrackingField:
                        return true;
                }
                return false;
            }
        }

        [NotMapped]
        /// <summary>
        /// Columns which require no mapping and are generated automatically for auditing.
        /// </summary>
        /// <returns></returns>
        public bool IsGeneratedColumn
        {
            get
            {
                switch (DeltaType)
                {
                    case EDeltaType.CreateAuditKey:
                    case EDeltaType.UpdateAuditKey:
                    case EDeltaType.CreateDate:
                    case EDeltaType.UpdateDate:
                    case EDeltaType.SurrogateKey:
                    case EDeltaType.ValidationStatus:
                        return true;
                }
                return false;
            }
        }

        /// <summary>
        /// Columns which indicate if the record is current.  These are the createdate, updatedate, iscurrentfield
        /// </summary>
        /// <returns></returns>
        public bool IsCurrentColumn()
        {
            switch (DeltaType)
            {
                case EDeltaType.ValidFromDate:
                case EDeltaType.ValidToDate:
                case EDeltaType.IsCurrentField:
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Updates the delta property of the table when creating a target table for a current source table.  
        /// SurrogateKey is translated to SourceSurrogateKey
        /// Audit fields are translated to nontrackingfield
        /// Encryption fields translated to NoPriview to stop re-encryption of already encrypted fields.
        /// 
        /// </summary>
        public void MapToTargetColumnProperties()
        {
            switch (DeltaType)
            {
                case EDeltaType.SurrogateKey:
                    DeltaType = EDeltaType.SourceSurrogateKey;
                    break;
                case EDeltaType.CreateDate:
                case EDeltaType.UpdateDate:
                case EDeltaType.CreateAuditKey:
                case EDeltaType.UpdateAuditKey:
                case EDeltaType.AzurePartitionKey:
                case EDeltaType.AzureRowKey:
                case EDeltaType.TimeStamp:
                case EDeltaType.ValidationStatus:
                    DeltaType = EDeltaType.NonTrackingField;
                    break;
            }

            switch (SecurityFlag)
            {
                case ESecurityFlag.FastEncrypt:
                    SecurityFlag = ESecurityFlag.FastEncrypted;
                    break;
               case ESecurityFlag.StrongEncrypt:
                   SecurityFlag = ESecurityFlag.StrongEncrypted;
                   break;
               case ESecurityFlag.OneWayHash:
                   SecurityFlag = ESecurityFlag.OnWayHashed;
                   break;
               case ESecurityFlag.FastDecrypt:
               case ESecurityFlag.StrongDecrypt:
                   SecurityFlag = ESecurityFlag.None;
                   break;
            }

            if (IsIncrementalUpdate)
                IsIncrementalUpdate = false;
        }

    }
}
