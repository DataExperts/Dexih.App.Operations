using System;
using System.Collections.Generic;
using dexih.repository;
using Microsoft.EntityFrameworkCore.Internal;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.operations
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum EImportAction
    {
        Replace, New, Leave, Skip, Delete
    }

    public class Import
    {
        public long HubKey { get; set; }
        public ImportObjects<DexihHubVariable> HubVariables { get; set; }
        public ImportObjects<DexihDatajob> Datajobs { get; set; }
        public ImportObjects<DexihDatalink> Datalinks { get; set; }
        public ImportObjects<DexihConnection> Connections { get; set; }
        public ImportObjects<DexihTable> Tables { get; set; }
        public ImportObjects<DexihColumnValidation> ColumnValidations { get; set; }
        public ImportObjects<DexihFileFormat> FileFormats { get; set; }
        public ImportObjects<DexihCustomFunction> CustomFunctions { get; set; }
        public ImportObjects<DexihRemoteAgentHub> RemoteAgentHubs { get; set; }
        public ImportObjects<DexihDatalinkTest> DatalinkTests { get; set; }
        public ImportObjects<DexihView> Views { get; set; }
        public ImportObjects<DexihApi> Apis { get; set; }
        public ImportObjects<DexihDashboard> Dashboards { get; set; }

        public List<string> Warnings { get; set; }

        public Import(long hubKey)
        {
            HubKey = hubKey;
            
            HubVariables = new ImportObjects<DexihHubVariable>();           
            Datajobs = new ImportObjects<DexihDatajob>();
            Datalinks = new ImportObjects<DexihDatalink>();
            Connections = new ImportObjects<DexihConnection>();
            Tables = new ImportObjects<DexihTable>();
            ColumnValidations = new ImportObjects<DexihColumnValidation>();
            CustomFunctions = new ImportObjects<DexihCustomFunction>();
            FileFormats = new ImportObjects<DexihFileFormat>();
            RemoteAgentHubs = new ImportObjects<DexihRemoteAgentHub>();
            DatalinkTests = new ImportObjects<DexihDatalinkTest>();
            Views = new ImportObjects<DexihView>();
            Apis = new ImportObjects<DexihApi>();
            Dashboards = new ImportObjects<DexihDashboard>();
            Warnings = new List<string>();
        }

        /// <summary>
        /// Adds one of the properties to the relevant area.
        /// </summary>
        /// <param name="property"></param>
        /// <param name="operation"></param>
        public bool Add(object property, EImportAction operation)
        {
            try
            {

                switch (property)
                {
                    case DexihHubVariable a:
                        HubVariables.Add(a, operation);
                        break;
                    case DexihDatajob a:
                        Datajobs.Add(a, operation);
                        break;
                    case DexihDatalink a:
                        Datalinks.Add(a, operation);
                        break;
                    case DexihConnection a:
                        Connections.Add(a, operation);
                        break;
                    case DexihTable a:
                        Tables.Add(a, operation);
                        break;
                    case DexihColumnValidation a:
                        ColumnValidations.Add(a, operation);
                        break;
                    case DexihCustomFunction a:
                        CustomFunctions.Add(a, operation);
                        break;
                    case DexihFileFormat a:
                        FileFormats.Add(a, operation);
                        break;
                    case DexihRemoteAgentHub a:
                        RemoteAgentHubs.Add(a, operation);
                        break;
                    case DexihDatalinkTest a:
                        DatalinkTests.Add(a, operation);
                        break;
                    case DexihView a:
                        Views.Add(a, operation);
                        break;
                    case DexihApi a:
                        Apis.Add(a, operation);
                        break;
                    case DexihDashboard a:
                        Dashboards.Add(a, operation);
                        break;
                    default:
                        return false;
                }

                return true;
            } catch (Exception ex)
            {
                throw new AggregateException($"Failed to add item.  {ex.Message}", ex);
            }
        }

        public void UpdateCache(DexihHub hub)
        {
            UpdateCacheItems(hub.DexihHubVariables, HubVariables);
            UpdateCacheItems(hub.DexihDatajobs, Datajobs);
            UpdateCacheItems(hub.DexihDatalinks, Datalinks);
            UpdateCacheItems(hub.DexihConnections, Connections);
            UpdateCacheItems(hub.DexihTables, Tables);
            UpdateCacheItems(hub.DexihColumnValidations, ColumnValidations);
            UpdateCacheItems(hub.DexihCustomFunctions, CustomFunctions);
            UpdateCacheItems(hub.DexihFileFormats, FileFormats);
            UpdateCacheItems(hub.DexihDatalinkTests, DatalinkTests);
            UpdateCacheItems(hub.DexihViews, Views);
            UpdateCacheItems(hub.DexihApis, Apis);
            UpdateCacheItems(hub.DexihDashboards, Dashboards);

            UpdateRemoteAgentHubCacheItems(hub);
        }

        public void UpdateCacheItems<T>(ICollection<T> existingItems, ImportObjects<T> updatedItems) where T : DexihHubNamedEntity
        {
            foreach (var updatedItem in updatedItems)
            {
                switch (updatedItem.ImportAction)
                {
                    case EImportAction.Replace:
                        foreach (var existingItem in existingItems)
                        {
                            if (existingItem.Key == updatedItem.Item.Key)
                            {
                                existingItems.Remove(existingItem);
                                break;
                            }
                        }
                        
                        existingItems.Add(updatedItem.Item);
                        break;
                    case EImportAction.New:
                        existingItems.Add(updatedItem.Item);
                        break;
                    case EImportAction.Leave:
                        break;
                    case EImportAction.Skip:
                        break;
                    case EImportAction.Delete:
                        foreach (var existingItem in existingItems)
                        {
                            if (existingItem.Key == updatedItem.Item.Key)
                            {
                                existingItems.Remove(existingItem);
                                break;
                            }
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }
        
        public void UpdateRemoteAgentHubCacheItems(DexihHub hub)
        {
            foreach (var updatedItem in RemoteAgentHubs)
            {
                switch (updatedItem.ImportAction)
                {
                    case EImportAction.Replace:
                        foreach (var existingItem in hub.DexihRemoteAgentHubs)
                        {
                            if (existingItem.RemoteAgentKey == updatedItem.Item.RemoteAgentKey)
                            {
                                hub.DexihRemoteAgentHubs.Remove(existingItem);
                                break;
                            }
                        }
                        
                        hub.DexihRemoteAgentHubs.Add(updatedItem.Item);
                        break;
                    case EImportAction.New:
                        hub.DexihRemoteAgentHubs.Add(updatedItem.Item);
                        break;
                    case EImportAction.Leave:
                        break;
                    case EImportAction.Skip:
                        break;
                    case EImportAction.Delete:
                        foreach (var existingItem in hub.DexihRemoteAgentHubs)
                        {
                            if (existingItem.RemoteAgentKey == updatedItem.Item.RemoteAgentKey)
                            {
                                hub.DexihRemoteAgentHubs.Remove(existingItem);
                                break;
                            }
                        }

                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }
            }
        }

        public bool Any()
        {
            return HubVariables.Any() || 
                   Datajobs.Any() || 
                   Datalinks.Any() || 
                   Connections.Any() || 
                   Tables.Any() ||
                   ColumnValidations.Any() || 
                   CustomFunctions.Any() || 
                   FileFormats.Any() || 
                   RemoteAgentHubs.Any() || 
                   DatalinkTests.Any() ||
                   Views.Any() ||
                   Apis.Any() ||
                   Dashboards.Any();
        }
    }

    public class ImportObjects<T> : List<ImportObject<T>>
    {
        public void Add(T item, EImportAction importAction)
        {
            base.Add(new ImportObject<T>(item, importAction));            
        }
    }

    public class ImportObject<T>
    {
        public ImportObject(T item, EImportAction importAction)
        {
            Item = item;
            ImportAction = importAction;
        }
        
        public T Item { get; set; }   
        public EImportAction ImportAction { get; set; }
    }
    
    
}