using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using dexih.functions;
using Dexih.Utils.DataType;


namespace dexih.repository
{
    /// <summary>
    /// Base class for table columns.  Inherited by DexihTableColumn and DexihDatalinkColumn
    /// </summary>
    [DataContract]
    // [Union(0, typeof(DexihDatalinkColumn))]
    // [Union(1, typeof(DexihDatalinkStepColumn))]
    // [Union(2, typeof(DexihTableColumn))]
    public class DexihColumnBase: DexihHubNamedEntity
    {
        [DataMember(Order = 7)]
        public int Position { get; set; }

        [DataMember(Order = 8)]
        public string LogicalName { get; set; }

        [DataMember(Order = 9)]
        public string ColumnGroup { get; set; }

        [DataMember(Order = 10)] 
        public ETypeCode DataType { get; set; } = ETypeCode.String;

        [DataMember(Order = 11)]
        public int? MaxLength { get; set; }

        [DataMember(Order = 12)]
        public int? Precision { get; set; }

        [DataMember(Order = 13)]
        public bool? IsUnicode { get; set; }

        [DataMember(Order = 14)]
        public int? Scale { get; set; }

        [DataMember(Order = 15)]
        public bool AllowDbNull { get; set; }

        [DataMember(Order = 16)] 
        public EDeltaType DeltaType { get; set; } = EDeltaType.TrackingField;

        [DataMember(Order = 17)]
        public string DefaultValue { get; set; }

        [DataMember(Order = 18)]
        public bool IsUnique { get; set; }

        [DataMember(Order = 19)]
        public bool IsMandatory { get; set; }

        [DataMember(Order = 20)]
        public bool IsIncrementalUpdate { get; set; }

        [DataMember(Order = 21)]
        public bool IsInput { get; set; }

        /// <summary>
        /// The number of array dimensions (zero for non array).
        /// </summary>
        [DataMember(Order = 22)]
        public int Rank { get; set; }

        public bool IsArray() => Rank > 0;


        [DataMember(Order = 23)] public ESecurityFlag SecurityFlag { get; set; } = ESecurityFlag.None;


        /// <summary>
        /// Is the column one form the source (vs. a value added column).
        /// </summary>
        /// <returns></returns>
        [NotMapped, IgnoreDataMember]
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

        /// <summary>
        /// Columns which require no mapping and are generated automatically for auditing.
        /// </summary>
        /// <returns></returns>
        [NotMapped, IgnoreDataMember]
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
                    case EDeltaType.AutoIncrement:
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

        public InputColumn ToInputColumn()
        {
            return new InputColumn()
            {
                Name = Name,
                LogicalName = LogicalName,
                Rank = Rank,
                DataType = DataType,
                Value = DefaultValue
            };
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
                case EDeltaType.AutoIncrement:
                    DeltaType = EDeltaType.SourceSurrogateKey;
                    break;
                case EDeltaType.CreateDate:
                case EDeltaType.UpdateDate:
                case EDeltaType.CreateAuditKey:
                case EDeltaType.UpdateAuditKey:
                case EDeltaType.PartitionKey:
                case EDeltaType.RowKey:
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
                   SecurityFlag = ESecurityFlag.OneWayHashed;
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
