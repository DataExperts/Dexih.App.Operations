using System;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using MessagePack;
using static dexih.functions.TableColumn;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    /// <summary>
    /// Base class for table columns.  Inherited by DexihTableColumn and DexihDatalinkColumn
    /// </summary>
    [MessagePackObject]
    [ProtoInherit(1000)]
    [MessagePack.Union(0, typeof(DexihDatalinkColumn))]
    [MessagePack.Union(1, typeof(DexihDatalinkStepColumn))]
    [MessagePack.Union(2, typeof(DexihTableColumn))]
    public class DexihColumnBase: DexihHubNamedEntity
    {
        [Key(7)]
        public int Position { get; set; }

        [Key(8)]
        public string LogicalName { get; set; }

        [Key(9)]
        public string ColumnGroup { get; set; }

        [Key(10)]
        public ETypeCode DataType { get; set; }

        [Key(11)]
        public int? MaxLength { get; set; }

        [Key(12)]
        public int? Precision { get; set; }

        [Key(13)]
        public bool? IsUnicode { get; set; }

        [Key(14)]
        public int? Scale { get; set; }

        [Key(15)]
        public bool AllowDbNull { get; set; }

        [Key(16)]
        public EDeltaType DeltaType { get; set; }

        [Key(17)]
        public string DefaultValue { get; set; }

        [Key(18)]
        public bool IsUnique { get; set; }

        [Key(19)]
        public bool IsMandatory { get; set; }

        [Key(20)]
        public bool IsIncrementalUpdate { get; set; }

        [Key(21)]
        public bool IsInput { get; set; }

        /// <summary>
        /// The number of array dimensions (zero for non array).
        /// </summary>
        [Key(22)]
        public int Rank { get; set; }

        public bool IsArray() => Rank > 0;


        [Key(23)]
        public ESecurityFlag SecurityFlag { get; set; }


        /// <summary>
        /// Is the column one form the source (vs. a value added column).
        /// </summary>
        /// <returns></returns>
        [NotMapped, IgnoreMember]
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
        [NotMapped, IgnoreMember]
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
