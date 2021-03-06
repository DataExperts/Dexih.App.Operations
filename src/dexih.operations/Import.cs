﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using dexih.repository;


namespace dexih.operations
{
    // [JsonConverter(typeof(StringEnumConverter))]
    public enum EImportAction
    {
        Replace = 1, New, Leave, Skip, Delete
    }

    [DataContract]
    public class Import
    {
        [DataMember(Order = 0)]
        public long HubKey { get; set; }

        [DataMember(Order = 1)]
        public ImportObjects<DexihHubVariable> HubVariables { get; set; }

        [DataMember(Order = 2)]
        public ImportObjects<DexihDatajob> Datajobs { get; set; }

        [DataMember(Order = 3)]
        public ImportObjects<DexihDatalink> Datalinks { get; set; }

        [DataMember(Order = 4)]
        public ImportObjects<DexihConnection> Connections { get; set; }

        [DataMember(Order = 5)]
        public ImportObjects<DexihTable> Tables { get; set; }

        [DataMember(Order = 6)]
        public ImportObjects<DexihColumnValidation> ColumnValidations { get; set; }

        [DataMember(Order = 7)]
        public ImportObjects<DexihFileFormat> FileFormats { get; set; }

        [DataMember(Order = 8)]
        public ImportObjects<DexihCustomFunction> CustomFunctions { get; set; }

        [DataMember(Order = 9)]
        public ImportObjects<DexihRemoteAgentHub> RemoteAgentHubs { get; set; }

        [DataMember(Order = 10)]
        public ImportObjects<DexihDatalinkTest> DatalinkTests { get; set; }

        [DataMember(Order = 11)]
        public ImportObjects<DexihView> Views { get; set; }

        [DataMember(Order = 12)]
        public ImportObjects<DexihApi> Apis { get; set; }

        [DataMember(Order = 13)]
        public ImportObjects<DexihDashboard> Dashboards { get; set; }

        [DataMember(Order = 14)]
        public ImportObjects<DexihListOfValues> ListOfValues { get; set; }

        [DataMember(Order = 15)]
        public ImportObjects<DexihTag> Tags { get; set; }
        
        [DataMember(Order = 16)]
        public ImportObjects<DexihTagObject> TagObjects { get; set; }
        
        [DataMember(Order = 17)]
        public List<string> Warnings { get; set; }

        public Import()
        {
            Initialize();
        }

        public Import(long hubKey)
        {
            HubKey = hubKey;
            Initialize();
        }

        private void Initialize()
        {
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
            ListOfValues = new ImportObjects<DexihListOfValues>();
            Tags = new ImportObjects<DexihTag>();
            TagObjects = new ImportObjects<DexihTagObject>();
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
                    case DexihListOfValues a:
                        ListOfValues.Add(a, operation);
                        break;
                    case DexihTag a:
                        Tags.Add(a, operation);
                        break;
                    case DexihTagObject a:
                        TagObjects.Add(a, operation);
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
            UpdateCacheItems(hub.DexihListOfValues, ListOfValues);
            UpdateCacheItems(hub.DexihCustomFunctions, CustomFunctions);
            UpdateCacheItems(hub.DexihFileFormats, FileFormats);
            UpdateCacheItems(hub.DexihConnections, Connections);
            UpdateCacheItems(hub.DexihTables, Tables);
            UpdateCacheItems(hub.DexihColumnValidations, ColumnValidations);
            UpdateCacheItems(hub.DexihDatalinks, Datalinks);
            UpdateCacheItems(hub.DexihDatajobs, Datajobs);
            UpdateCacheItems(hub.DexihDatalinkTests, DatalinkTests);
            UpdateCacheItems(hub.DexihViews, Views);
            UpdateCacheItems(hub.DexihApis, Apis);
            UpdateCacheItems(hub.DexihDashboards, Dashboards);
            UpdateCacheItems(hub.DexihListOfValues, ListOfValues);
            UpdateCacheItems(hub.DexihTags, Tags);

            UpdateTagObjectCacheItems(hub);
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
        
        public void UpdateTagObjectCacheItems(DexihHub hub)
        {
            foreach (var updatedItem in TagObjects)
            {
                switch (updatedItem.ImportAction)
                {
                    case EImportAction.Replace:
                        foreach (var existingItem in hub.DexihTagObjects)
                        {
                            if (existingItem.TagKey == updatedItem.Item.TagKey && existingItem.ObjectKey == updatedItem.Item.ObjectKey && existingItem.ObjectType == updatedItem.Item.ObjectType)
                            {
                                hub.DexihTagObjects.Remove(existingItem);
                                break;
                            }
                        }
                        
                        hub.DexihTagObjects.Add(updatedItem.Item);
                        break;
                    case EImportAction.New:
                        hub.DexihTagObjects.Add(updatedItem.Item);
                        break;
                    case EImportAction.Leave:
                        break;
                    case EImportAction.Skip:
                        break;
                    case EImportAction.Delete:
                        foreach (var existingItem in hub.DexihTagObjects)
                        {
                            if (existingItem.TagKey == updatedItem.Item.TagKey && existingItem.ObjectKey == updatedItem.Item.ObjectKey && existingItem.ObjectType == updatedItem.Item.ObjectType)
                            {
                                hub.DexihTagObjects.Remove(existingItem);
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
                   Dashboards.Any() ||
                   ListOfValues.Any() ||
                   Tags.Any() || 
                   TagObjects.Any();
            
        }
    }

    public class ImportObjects<T> : List<ImportObject<T>>
    {
        public void Add(T item, EImportAction importAction)
        {
            base.Add(new ImportObject<T>(item, importAction));            
        }
    }

    [DataContract]
    public class ImportObject<T>
    {
        public ImportObject()
        {
        }

        public ImportObject(T item, EImportAction importAction)
        {
            Item = item;
            ImportAction = importAction;
        }
        
        [DataMember(Order = 0)]
        public T Item { get; set; }

        [DataMember(Order = 1)]
        public EImportAction ImportAction { get; set; }
    }
    
    
}