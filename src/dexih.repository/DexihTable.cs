using System;
using System.Collections.Generic;

using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using static dexih.transforms.Connection;
using dexih.transforms;
using dexih.transforms.File;
using Dexih.Utils.DataType;


namespace dexih.repository
{
	[DataContract]
    public class DexihTable : DexihHubNamedEntity
    {
        public DexihTable()
        {
            DexihTableColumns = new HashSet<DexihTableColumn>();
            DexihTargetTables = new HashSet<DexihDatalinkTarget>();
            DexihViews = new HashSet<DexihView>();
            EntityStatus = new EntityStatus();
        }

        [DataMember(Order = 7)]
        public long ConnectionKey { get; set; }

        [DataMember(Order = 8)]
        public string Schema {get; set; }

        [DataMember(Order = 9)]
        public string BaseTableName { get; set; }

        [DataMember(Order = 10)]
        public string LogicalName { get; set; }

        [DataMember(Order = 11)] 
        public Table.ETableType TableType { get; set; } = Table.ETableType.Table;

        [DataMember(Order = 12)]
        public string SourceConnectionName { get; set; }

        [DataMember(Order = 13)]
        public long? FileFormatKey { get; set; }

        [DataMember(Order = 14)]
        public string RejectedTableName { get; set; }

        // [DataMember(Order = 15)]
        // public bool UseQuery { get; set; }

        [DataMember(Order = 15)]
        public string QueryString { get; set; }

        [DataMember(Order = 16)]
        public string RowPath { get; set; }

        [DataMember(Order = 17)]
        public ETypeCode FormatType { get; set; } = ETypeCode.Json;

        [DataMember(Order = 18)]
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
        
        [JsonIgnore, IgnoreDataMember]
        public string SortColumnKeysString { get; set; }
        
        [DataMember(Order = 19)]
        public bool AutoManageFiles { get; set; }

        [DataMember(Order = 20)]
        public bool UseCustomFilePaths { get; set; }

        [DataMember(Order = 21)]
        public string FileRootPath { get; set; }

        [DataMember(Order = 22)]
        public string FileIncomingPath { get; set; }

        [DataMember(Order = 23)]
        public string FileOutgoingPath { get; set; }

        [DataMember(Order = 24)]
        public string FileProcessedPath { get; set; }

        [DataMember(Order = 25)]
        public string FileRejectedPath { get; set; }

        [DataMember(Order = 26)]
        public string FileMatchPattern { get; set; }

        [DataMember(Order = 27)]
        public string RestfulUri { get; set; }

        [DataMember(Order = 28)]
        [NotMapped]
	    public int MaxImportLevels { get; set; }

        [DataMember(Order = 29)]
        public bool IsVersioned { get; set; }

        [DataMember(Order = 30)]
        public bool IsShared { get; set; }
		
        [DataMember(Order = 31)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [DataMember(Order = 32), NotMapped]
        public string FileSample {get;set;}
        
        [DataMember(Order = 33)]
        public ICollection<DexihTableColumn> DexihTableColumns { get ; set; }

        
		[NotMapped, JsonIgnore, CopyIgnore, IgnoreDataMember]
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
		
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public ICollection<DexihView> DexihViews { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public ICollection<DexihDatalinkTarget> DexihTargetTables { get; set; }

	    [JsonIgnore, CopyIgnore, IgnoreDataMember]
	    public ICollection<DexihDatalinkTable> DexihDatalinkTables { get; set; }

	    [JsonIgnore, CopyIgnore, IgnoreDataMember]
	    public ICollection<DexihListOfValues> DexihListOfValues { get; set; }
	    
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihConnection Connection { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihFileFormat FileFormat { get; set; }

        [JsonIgnore, CopyIgnore, NotMapped, IgnoreDataMember]
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

				        if (fileFormat == null)
				        {
					        throw new RepositoryException(
						        $"The file format for the table {Name} with key {FileFormatKey} could not be found.");
				        }

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


            foreach (var dbColumn in DexihTableColumns.Where(c => c.IsValid && c.DeltaType != EDeltaType.IgnoreField).OrderBy(c => c.Position))
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
