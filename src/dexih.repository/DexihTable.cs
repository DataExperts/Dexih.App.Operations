﻿using System.Collections.Generic;
using Newtonsoft.Json;
using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using static dexih.transforms.Connection;
using System;
using dexih.transforms;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    public partial class DexihTable : DexihBaseEntity
    {
        public DexihTable()
        {
            DexihTableColumns = new HashSet<DexihTableColumn>();
            DexihTargetTables = new HashSet<DexihDatalink>();
            EntityStatus = new EntityStatus();
        }

		[CopyCollectionKey((long)0, true)]
        public long TableKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
        public long ConnectionKey { get; set; }
        public string Name { get; set; }
		public string Schema {get; set; }
        public string BaseTableName { get; set; }
        public string LogicalName { get; set; }
        public string SourceConnectionName { get; set; }
        public string Description { get; set; }
        public long? FileFormatKey { get; set; }
        public string RejectedTableName { get; set; }
	    public bool UseQuery { get; set; }
	    public string QueryString { get; set; }
        public string RowPath { get; set; }

        public ETypeCode FormatType { get; set; } = ETypeCode.Json;

	    public long[] SortColumnKeys { get; set; } = new long[0];
        public bool AutoManageFiles { get; set; }

		public bool UseCustomFilePaths { get; set; }
        public string FileRootPath { get; set; }
        public string FileIncomingPath { get; set; }
        public string FileOutgoingPath { get; set; }
        public string FileProcessedPath { get; set; }
        public string FileRejectedPath { get; set; }
		public string FileMatchPattern { get; set; }
	    
        public string RestfulUri { get; set; }
	    
	    [NotMapped]
	    public int MaxImportLevels { get; set; }
        
	    public bool IsVersioned { get; set; }
//        public bool IsInternal { get; set; }
		public bool IsShared { get; set; }


		[NotMapped, JsonIgnore, CopyIgnore]
		public string[] OutputSortFields {
			get {
				var fields = new List<string>();
				if(SortColumnKeys != null && DexihTableColumns != null) 
				{
					foreach(var sortColumnKey in SortColumnKeys)
					{
						var column = DexihTableColumns.FirstOrDefault(c => c.ColumnKey == sortColumnKey && IsValid);
						if(column != null) 
						{
							fields.Add(column.Name);
						}
					}
				}
				return fields.ToArray();
			}
		}

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [NotMapped]
        public string FileSample {get;set;}

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalink> DexihTargetTables { get; set; }

	    [JsonIgnore, CopyIgnore]
	    public virtual ICollection<DexihDatalinkTable> DexihDatalinkTables { get; set; }

        public virtual ICollection<DexihTableColumn> DexihTableColumns { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihConnection Connection { get; set; }

	    [CopyReference]
        public virtual DexihFileFormat FileFormat { get; set; }

        public Table GetTable(Connection connection, InputColumn[] inputColumns, TransformSettings transformSettings)
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
				        ((FlatFile)table).FileConfiguration = FileFormat?.GetFileFormat();
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
                
					if (transformSettings.HubVariables != null && hubVariablesArray.Any())
					{
						var hubVariablesManager = new HubVariablesManager(transformSettings, hubVariablesArray);

						flatFile.FileIncomingPath = hubVariablesManager.InsertHubVariables(flatFile.FileIncomingPath, false);
						flatFile.FileMatchPattern = hubVariablesManager.InsertHubVariables(flatFile.FileMatchPattern, false);
						flatFile.FileOutgoingPath = hubVariablesManager.InsertHubVariables(flatFile.FileOutgoingPath, false);
						flatFile.FileProcessedPath = hubVariablesManager.InsertHubVariables(flatFile.FileProcessedPath, false);
						flatFile.FileRejectedPath = hubVariablesManager.InsertHubVariables(flatFile.FileRejectedPath, false);
						flatFile.FileRootPath = hubVariablesManager.InsertHubVariables(flatFile.FileRootPath, false);
					}

					break;
			   case WebService restFunction:
                
				   if (transformSettings.HubVariables != null && hubVariablesArray.Any())
				   {
					   var hubVariablesManager = new HubVariablesManager(transformSettings, hubVariablesArray);

					   restFunction.RestfulUri = hubVariablesManager.InsertHubVariables(restFunction.RestfulUri, false);
					   restFunction.RowPath = hubVariablesManager.InsertHubVariables(restFunction.RowPath, false);
				   }

				   break;
	        }


            foreach (var dbColumn in DexihTableColumns.Where(c => c.IsValid).OrderBy(c => c.Position))
            {
                table.Columns.Add(dbColumn.GetTableColumn(inputColumns));
            }
	        

            return table;
        }

        public Table GetRejectedTable(Connection connection, TransformSettings transformSettings)
        {
            var table = GetTable(connection, null, transformSettings);
            return table.GetRejectedTable(RejectedTableName);
        }
    }

}
