using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.Crypto;
using Dexih.Utils.CopyProperties;
using dexih.transforms;
using System.Linq;
using dexih.functions;

namespace dexih.repository
{
    public class DexihConnection : DexihBaseEntity
    {
        #region Enums
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EConnectionPurpose
        {
            Source = 0,
            Managed = 1,
            Target = 2,
            Internal = 3
        }
         
        #endregion

        public DexihConnection()
        {
            DexihTables = new HashSet<DexihTable>();
        }

        [CopyCollectionKey((long)0, true)]
        public long ConnectionKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        public string ConnectionAssemblyName { get; set; }
        public string ConnectionClassName { get; set; }
        
        public EConnectionPurpose Purpose { get; set; }

        public string Name { get; set; }
        public string Description { get; set; }
        public string Server { get; set; }
        public bool UseWindowsAuth { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }
        public bool UsePasswordVariable { get; set; }
        public string DefaultDatabase { get; set; }
        public string Filename { get; set; }
        public bool UseConnectionString { get; set; }
        public string ConnectionString { get; set; }
        public bool UseConnectionStringVariable { get; set; }
 //       public bool IsInternal { get; set; }
        public bool EmbedTableKey { get; set; }
 
        //these store the raw (unencrypted values) and are not saved to the database.
        [NotMapped]
        public string PasswordRaw { get; set; }
        [NotMapped]
        public string ConnectionStringRaw { get; set; }

        public ICollection<DexihTable> DexihTables { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalink> DexihDatalinkAuditConnections { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatajob> DexihDatajobAuditConnections { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihHub Hub { get; set; }

        public string GetPassword(string key, int iterations)
        {
            if(string.IsNullOrEmpty(PasswordRaw))
            {
                if(string.IsNullOrEmpty(Password))
                {
                    return "";
                }

                if (UsePasswordVariable)
                {
                    return Password;
                }
                
                return EncryptString.Decrypt(Password, key, iterations);
            }
            return PasswordRaw;
        }

        public string GetConnectionString(string key, int iterations)
        {
            if(string.IsNullOrEmpty(ConnectionStringRaw))
            {
                if(string.IsNullOrEmpty(ConnectionString))
                {
                    return "";
                }

                if (UseConnectionStringVariable)
                {
                    return ConnectionString;
                }
                return EncryptString.Decrypt(ConnectionString, key, iterations);
            }
            return ConnectionStringRaw;
        }

        public bool Encrypt(string key, int iterations)
        {
            if(!string.IsNullOrEmpty(PasswordRaw))
            {
                Password = EncryptString.Encrypt(PasswordRaw, key, iterations);
                PasswordRaw = null;
            } 
            if(!string.IsNullOrEmpty(PasswordRaw))
            {
                ConnectionString = EncryptString.Encrypt(ConnectionStringRaw, key, iterations);
                ConnectionStringRaw = null;
            }

            return true;
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
                this.CopyProperties(connection, true);

                connection.Password = GetPassword(transformSettings.RemoteSettings.AppSettings.EncryptionKey, transformSettings.RemoteSettings.SystemSettings.EncryptionIterations);
                connection.ConnectionString = GetConnectionString(transformSettings.RemoteSettings.AppSettings.EncryptionKey, transformSettings.RemoteSettings.SystemSettings.EncryptionIterations);

                if (transformSettings.HasVariables())
                {
                    connection.ConnectionString = transformSettings.InsertHubVariables(connection.ConnectionString, true);
                    connection.Server = transformSettings.InsertHubVariables(connection.Server, false);
                    connection.Password = transformSettings.InsertHubVariables(connection.Password, true);
                    connection.Username = transformSettings.InsertHubVariables(connection.Username, false);
                    connection.DefaultDatabase = transformSettings.InsertHubVariables(connection.DefaultDatabase, false);
                    connection.Filename = transformSettings.InsertHubVariables(connection.Filename, false);
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
