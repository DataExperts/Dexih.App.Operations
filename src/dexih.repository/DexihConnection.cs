using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.Crypto;
using Dexih.Utils.CopyProperties;
using dexih.transforms;


namespace dexih.repository
{
    [DataContract]
    public class DexihConnection : DexihHubNamedEntity
    {
        #region Enums

         
        #endregion

        public DexihConnection()
        {
            DexihTables = new HashSet<DexihTable>();
        }


        [DataMember(Order = 7)]
        public string ConnectionAssemblyName { get; set; }

        [DataMember(Order = 8)]
        public string ConnectionClassName { get; set; }

        [DataMember(Order = 9)]
        public EConnectionPurpose Purpose { get; set; }

        [DataMember(Order = 10)]
        public string Server { get; set; }

        [DataMember(Order = 11)]
        public bool UseWindowsAuth { get; set; }

        [DataMember(Order = 12)]
        public string Username { get; set; }

        [DataMember(Order = 13)]
        public string Password { get; set; }

        [DataMember(Order = 14)]
        public bool UsePasswordVariable { get; set; }

        [DataMember(Order = 15)]
        public string DefaultDatabase { get; set; }

        [DataMember(Order = 16)]
        public string Filename { get; set; }

        [DataMember(Order = 17)]
        public bool UseConnectionString { get; set; }

        [DataMember(Order = 18)]
        public string ConnectionString { get; set; }

        [DataMember(Order = 19)]
        public bool UseConnectionStringVariable { get; set; }

        [DataMember(Order = 20)]
        public bool EmbedTableKey { get; set; }

        [DataMember(Order = 21)] 
        public int ConnectionTimeout { get; set; } = 30;

        [DataMember(Order = 22)] 
        public int CommandTimeout { get; set; } = 60;

        //these store the raw (unencrypted values) and are not saved to the database.
//        [DataMember(Order = 21)]
//        [NotMapped]
//        public string PasswordRaw { get; set; }

//        [DataMember(Order = 22)]
//        [NotMapped]
//        public string ConnectionStringRaw { get; set; }

        [JsonIgnore, IgnoreDataMember]
        public ICollection<DexihTable> DexihTables { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public ICollection<DexihDatalink> DexihDatalinkAuditConnections { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public ICollection<DexihDatajob> DexihDatajobAuditConnections { get; set; }

        public string GetPassword(string key, int iterations)
        {
//            if(string.IsNullOrEmpty(PasswordRaw))
//            {
//                if(string.IsNullOrEmpty(Password))
//                {
//                    return "";
//                }
//
//                if (UsePasswordVariable)
//                {
//                    return Password;
//                }
                
                return EncryptString.Decrypt(Password, key, iterations);
//            }
//            return PasswordRaw;
        }

        public string GetConnectionString(string key, int iterations)
        {
//            if(string.IsNullOrEmpty(ConnectionStringRaw))
//            {
//                if(string.IsNullOrEmpty(ConnectionString))
//                {
//                    return "";
//                }
//
//                if (UseConnectionStringVariable)
//                {
//                    return ConnectionString;
//                }
                return EncryptString.Decrypt(ConnectionString, key, iterations);
//            }
//            return ConnectionStringRaw;
        }
        
        public Connection GetConnection(TransformSettings transformSettings)
        {
            try
            {
                var connectionReference = Connections.GetConnection(ConnectionClassName, ConnectionAssemblyName);
                
                if (!transformSettings.RemoteSettings.Permissions.AllowLocalFiles && connectionReference.RequiresLocalStorage)
                {
                    throw new RepositoryException($"The connection {connectionReference.Name} can not be used on this remote agent as local file access is forbidden.");
                }

                var connection = connectionReference.GetConnection();
                connection.ClientFactory = transformSettings.ClientFactory;
                
                this.CopyProperties(connection, true);
                
                if (!UsePasswordVariable)
                {
                    connection.Password = GetPassword(transformSettings.RemoteSettings.AppSettings.EncryptionKey,
                        transformSettings.RemoteSettings.SystemSettings.EncryptionIterations);
                }

                if (!UseConnectionStringVariable)
                {
                    connection.ConnectionString = GetConnectionString(
                        transformSettings.RemoteSettings.AppSettings.EncryptionKey,
                        transformSettings.RemoteSettings.SystemSettings.EncryptionIterations);
                }

                if (transformSettings.HasVariables())
                {
                    connection.ConnectionString = transformSettings.InsertHubVariables(connection.ConnectionString);
                    connection.Server = transformSettings.InsertHubVariables(connection.Server);
                    connection.Password = transformSettings.InsertHubVariables(connection.Password);
                    connection.Username = transformSettings.InsertHubVariables(connection.Username);
                    connection.DefaultDatabase = transformSettings.InsertHubVariables(connection.DefaultDatabase);
                    connection.Filename = transformSettings.InsertHubVariables(connection.Filename);
                }

                connection.FilePermissions = transformSettings.RemoteSettings.Permissions.GetFilePermissions();

                return connection;
            }
            catch (Exception ex)
            {
                throw new RepositoryException($"Get connection failed.  {ex.Message}", ex);
            }
        }
    }
}
