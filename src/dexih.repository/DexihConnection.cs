using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.Crypto;
using Dexih.Utils.CopyProperties;
using dexih.transforms;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihConnection : DexihHubNamedEntity
    {
        #region Enums

         
        #endregion

        public DexihConnection()
        {
            DexihTables = new HashSet<DexihTable>();
        }


        [ProtoMember(1)]
        public string ConnectionAssemblyName { get; set; }

        [ProtoMember(2)]
        public string ConnectionClassName { get; set; }

        [ProtoMember(3)]
        public EConnectionPurpose Purpose { get; set; }

        [ProtoMember(4)]
        public string Server { get; set; }

        [ProtoMember(5)]
        public bool UseWindowsAuth { get; set; }

        [ProtoMember(6)]
        public string Username { get; set; }

        [ProtoMember(7)]
        public string Password { get; set; }

        [ProtoMember(8)]
        public bool UsePasswordVariable { get; set; }

        [ProtoMember(9)]
        public string DefaultDatabase { get; set; }

        [ProtoMember(10)]
        public string Filename { get; set; }

        [ProtoMember(11)]
        public bool UseConnectionString { get; set; }

        [ProtoMember(12)]
        public string ConnectionString { get; set; }

        [ProtoMember(13)]
        public bool UseConnectionStringVariable { get; set; }

        [ProtoMember(14)]
        public bool EmbedTableKey { get; set; }

        //these store the raw (unencrypted values) and are not saved to the database.
        [ProtoMember(15)]
        [NotMapped]
        public string PasswordRaw { get; set; }

        [ProtoMember(16)]
        [NotMapped]
        public string ConnectionStringRaw { get; set; }

        [JsonIgnore]
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
