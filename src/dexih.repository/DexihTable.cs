using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using static dexih.transforms.Connection;
using dexih.transforms;
using static Dexih.Utils.DataType.DataType;
using ProtoBuf;

namespace dexih.repository
{
	[ProtoContract]
    public partial class DexihTable : DexihHubNamedEntity
    {
        public DexihTable()
        {
            DexihTableColumns = new HashSet<DexihTableColumn>();
            DexihTargetTables = new HashSet<DexihDatalinkTarget>();
            DexihViews = new HashSet<DexihView>();
            EntityStatus = new EntityStatus();
        }

        [ProtoMember(1)]
        public long ConnectionKey { get; set; }

        [ProtoMember(2)]
        public string Schema {get; set; }

        [ProtoMember(3)]
        public string BaseTableName { get; set; }

        [ProtoMember(4)]
        public string LogicalName { get; set; }

        [ProtoMember(5)]
        public Table.ETableType TableType { get; set; }

        [ProtoMember(6)]
        public string SourceConnectionName { get; set; }

        [ProtoMember(7)]
        public long? FileFormatKey { get; set; }

        [ProtoMember(8)]
        public string RejectedTableName { get; set; }

        [ProtoMember(9)]
        public bool UseQuery { get; set; }

        [ProtoMember(10)]
        public string QueryString { get; set; }

        [ProtoMember(11)]
        public string RowPath { get; set; }

        [ProtoMember(12)]
        public ETypeCode FormatType { get; set; } = ETypeCode.Json;

        [ProtoMember(13)]
        public List<long> SortColumnKeys { get; set; } = new List<long>();

        [ProtoMember(14)]
        public bool AutoManageFiles { get; set; }

        [ProtoMember(15)]
        public bool UseCustomFilePaths { get; set; }

        [ProtoMember(16)]
        public string FileRootPath { get; set; }

        [ProtoMember(17)]
        public string FileIncomingPath { get; set; }

        [ProtoMember(18)]
        public string FileOutgoingPath { get; set; }

        [ProtoMember(19)]
        public string FileProcessedPath { get; set; }

        [ProtoMember(20)]
        public string FileRejectedPath { get; set; }

        [ProtoMember(21)]
        public string FileMatchPattern { get; set; }

        [ProtoMember(22)]
        public string RestfulUri { get; set; }

        [ProtoMember(23)]
        [NotMapped]
	    public int MaxImportLevels { get; set; }

        [ProtoMember(24)]
        public bool IsVersioned { get; set; }

        [ProtoMember(25)]
        public bool IsShared { get; set; }
		
		[NotMapped, JsonIgnore, CopyIgnore]
		public List<string> OutputSortFields {
			get {
				var fields = new List<string>();
				if(SortColumnKeys != null && DexihTableColumns != null) 
				{
					foreach(var sortColumnKey in SortColumnKeys)
					{
						var column = DexihTableColumns.FirstOrDefault(c => c.Key == sortColumnKey && IsValid);
						if(column != null) 
						{
							fields.Add(column.Name);
						}
					}
				}
				return fields.ToList();
			}
		}

        [ProtoMember(26)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [ProtoMember(27)]
        [NotMapped]
        public string FileSample {get;set;}

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihView> DexihViews { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTarget> DexihTargetTables { get; set; }

	    [JsonIgnore, CopyIgnore]
	    public ICollection<DexihDatalinkTable> DexihDatalinkTables { get; set; }

        [ProtoMember(28)]
        public ICollection<DexihTableColumn> DexihTableColumns { get ; set; }

        [JsonIgnore, CopyIgnore]
        public DexihConnection Connection { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihFileFormat FileFormat { get; set; }

        [JsonIgnore, CopyIgnore, NotMapped]
        public override long ParentKey => ConnectionKey;

	    public Table GetTable(DexihHub hub, Connection connection, TransformSettings transformSettings)
	    {
		    return GetTable(hub, connection, (InputColumn[]) null, transformSettings);
	    }
	    
	    public Table GetTable(DexihHub hub, Connection connection,  IEnumerable<DexihColumnBase> inputColumns, TransformSettings transformSettings)
	    {
		    var inputs = inputColumns.Select(c => c.ToInputColumn()).ToArray();
		    return GetTable(hub, connection, inputs, transformSettings);
	    }

        public Table GetTable(DexihHub hub, Connection connection, InputColumn[] inputColumns, TransformSettings transformSettings)
        {
	        Table table;

	        if (connection == null)
	        {
		        table = new Table();
	        }
	        else
	        {
		        var connectionReference = Connections.GetConnection(connection.GetType());
		        switch (connectionReference.ConnectionCategory)
		        {
			        case EConnectionCategory.File:
				        table = new FlatFile();
				        var fileFormat = hub.DexihFileFormats.SingleOrDefault(f => f.Key == FileFormatKey);
				        ((FlatFile)table).FileConfiguration =  fileFormat?.GetFileFormat();
				        break;
			        case EConnectionCategory.WebService:
				        table = new WebService();
				        break;
			        default:
				        table = new Table();
				        break;
		        }
	        }
	        
	        // create a temporary copy and exclude the fileFormat (which is different between DexihTable & Table)
	        this.CopyProperties(table, false);

	        // shift to array to avoid multiple enumerations.
	        var hubVariablesArray = transformSettings.HubVariables?.ToArray();

	        switch (table)
	        {
				case FlatFile flatFile:
                
					if (transformSettings.HasVariables())
					{
						flatFile.FileIncomingPath = transformSettings.InsertHubVariables(flatFile.FileIncomingPath, false);
						flatFile.FileMatchPattern = transformSettings.InsertHubVariables(flatFile.FileMatchPattern, false);
						flatFile.FileOutgoingPath = transformSettings.InsertHubVariables(flatFile.FileOutgoingPath, false);
						flatFile.FileProcessedPath = transformSettings.InsertHubVariables(flatFile.FileProcessedPath, false);
						flatFile.FileRejectedPath = transformSettings.InsertHubVariables(flatFile.FileRejectedPath, false);
						flatFile.FileRootPath = transformSettings.InsertHubVariables(flatFile.FileRootPath, false);
					}

					break;
			   case WebService restFunction:
                
				   if (transformSettings.HasVariables())
				   {
					   restFunction.RestfulUri = transformSettings.InsertHubVariables(restFunction.RestfulUri, false);
					   restFunction.RowPath = transformSettings.InsertHubVariables(restFunction.RowPath, false);
				   }

				   break;
	        }


            foreach (var dbColumn in DexihTableColumns.Where(c => c.IsValid).OrderBy(c => c.Position))
            {
                table.Columns.Add(dbColumn.GetTableColumn(inputColumns));
            }
	        

            return table;
        }

        public Table GetRejectedTable(DexihHub hub, Connection connection, TransformSettings transformSettings)
        {
            var table = GetTable(hub, connection, transformSettings);
            return table.GetRejectedTable(RejectedTableName);
        }
    }

}
