using System;
using System.Collections.Generic;

using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using static dexih.transforms.Connection;
using dexih.transforms;
using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.repository
{
	[MessagePackObject]
    public partial class DexihTable : DexihHubNamedEntity
    {
        public DexihTable()
        {
            DexihTableColumns = new HashSet<DexihTableColumn>();
            DexihTargetTables = new HashSet<DexihDatalinkTarget>();
            DexihViews = new HashSet<DexihView>();
            EntityStatus = new EntityStatus();
        }

        [Key(7)]
        public long ConnectionKey { get; set; }

        [Key(8)]
        public string Schema {get; set; }

        [Key(9)]
        public string BaseTableName { get; set; }

        [Key(10)]
        public string LogicalName { get; set; }

        [Key(11)] 
        public Table.ETableType TableType { get; set; } = Table.ETableType.Table;

        [Key(12)]
        public string SourceConnectionName { get; set; }

        [Key(13)]
        public long? FileFormatKey { get; set; }

        [Key(14)]
        public string RejectedTableName { get; set; }

        [Key(15)]
        public bool UseQuery { get; set; }

        [Key(16)]
        public string QueryString { get; set; }

        [Key(17)]
        public string RowPath { get; set; }

        [Key(18)]
        public ETypeCode FormatType { get; set; } = ETypeCode.Json;

        [Key(19)]
        [NotMapped, CopyIgnore]
        public long[] SortColumnKeys
        {
	        get
	        {
		        if (string.IsNullOrEmpty(SortColumnKeysString))
		        {
			        return null;
		        }
		        else
		        {
			        return SortColumnKeysString.Split(',')
				        .Select(long.Parse).ToArray();
		        }
	        }
	        set
	        {
		        if (value == null)
		        {
			        SortColumnKeysString = null;
		        }
		        else
		        {
			        SortColumnKeysString = String.Join(",", value.Select(c => c.ToString()));
		        }
	        }
	        
        }
        
        [JsonIgnore]
        public string SortColumnKeysString { get; set; }
        
        [Key(20)]
        public bool AutoManageFiles { get; set; }

        [Key(21)]
        public bool UseCustomFilePaths { get; set; }

        [Key(22)]
        public string FileRootPath { get; set; }

        [Key(23)]
        public string FileIncomingPath { get; set; }

        [Key(24)]
        public string FileOutgoingPath { get; set; }

        [Key(25)]
        public string FileProcessedPath { get; set; }

        [Key(26)]
        public string FileRejectedPath { get; set; }

        [Key(27)]
        public string FileMatchPattern { get; set; }

        [Key(28)]
        public string RestfulUri { get; set; }

        [Key(29)]
        [NotMapped]
	    public int MaxImportLevels { get; set; }

        [Key(30)]
        public bool IsVersioned { get; set; }

        [Key(31)]
        public bool IsShared { get; set; }
		
		[NotMapped, JsonIgnore, CopyIgnore, IgnoreMember]
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

        [Key(32)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [Key(33), NotMapped]
        public string FileSample {get;set;}

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public ICollection<DexihView> DexihViews { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public ICollection<DexihDatalinkTarget> DexihTargetTables { get; set; }

	    [JsonIgnore, CopyIgnore, IgnoreMember]
	    public ICollection<DexihDatalinkTable> DexihDatalinkTables { get; set; }

	    [JsonIgnore, CopyIgnore, IgnoreMember]
	    public ICollection<DexihListOfValues> DexihListOfValues { get; set; }

	    
        [Key(34)]
        public ICollection<DexihTableColumn> DexihTableColumns { get ; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihConnection Connection { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihFileFormat FileFormat { get; set; }

        [JsonIgnore, CopyIgnore, NotMapped, IgnoreMember]
        public override long ParentKey => ConnectionKey;

        public override void ResetKeys()
        {
	        Key = 0;
            
	        foreach (var column  in DexihTableColumns)
	        {
		        column .ResetKeys();
	        }
        }
        
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
				        var fileFormat = hub.DexihFileFormats.SingleOrDefault(f => f.IsValid && f.Key == FileFormatKey);
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
