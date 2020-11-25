using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using System.Linq;
using System;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.Mapping;
using dexih.transforms.View;
using Dexih.Utils.DataType;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Identity;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;


namespace dexih.repository
{
	public class DexihRepositoryContext : IdentityDbContext<ApplicationUser>
	{
		public EDatabaseType DatabaseType { get; set; }
        public IHostingEnvironment Environment { get; set; }
        // public long HubKey { get; set; } = 0;

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
		{
#if DEBUG
            optionsBuilder.EnableSensitiveDataLogging(true);
#endif
        }

		public DexihRepositoryContext(DbContextOptions options) : base(options)
		{
		}

//        /// <summary>
//        /// Adds the hubKey value to all saved entities.
//        /// </summary>
//        /// <param name="hubKey"></param>
//        /// <param name="acceptAllChangesOnSuccess"></param>
//        /// <param name="cancellationToken"></param>
//        /// <returns></returns>
//        public async Task<int> SaveHub(long hubKey, bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default(CancellationToken))
//        {
//            var entities = ChangeTracker.Entries().Where(x => x.Entity is DexihHubEntity && (x.State == EntityState.Added || x.State == EntityState.Modified));
//
//            // before saving force all entities to contain the correct hub hbu.
//            foreach (var entity in entities)
//            {
//                if (entity.Entity is DexihHubEntity hubEntity)
//                {
//                    hubEntity.HubKey = hubKey;
//                    var originalValues = await entity.GetDatabaseValuesAsync(cancellationToken);
//
//                    if (entity.State == EntityState.Modified)
//                    {
//                        if (originalValues == null)
//                        {
//                            throw new RepositoryException($"The entity {hubEntity.GetType()} could not be modified as a version does not exist in the repository.");
//                        }
//                        // check hubkey hasn't changed since original.  This could impact an entity in another hub and causes an immediate stop.
//                        if (!Object.Equals(originalValues[nameof(DexihHubEntity.HubKey)], entity.CurrentValues[nameof(DexihHubEntity.HubKey)]))
//                        {
//                            if (hubEntity is DexihHubNamedEntity hubNamedEntity)
//                            {
//                                throw new SecurityException($"The hub_key on the original entity and the updated entity have changed.  The entity was type:{hubEntity.GetType()}, key: {hubNamedEntity.Key}, name: {hubNamedEntity.Name}.");
//                            }
//                            else
//                            {
//                                throw new SecurityException($"The hub_key on the original entity and the updated entity have changed.  The entity was type:{hubEntity.GetType()}.");    
//                            }
//                            
//                        }
//                    }
//
//                    if (entity.State == EntityState.Added && originalValues != null)
//                    {
//                        throw new RepositoryException($"The entity {hubEntity.GetType()} could not be added as a version already exists in the repository.");
//                    }
//                }
//            }
//
//            return await SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
//        }

        public override int SaveChanges()
		{
			AddTimestamps();
			return base.SaveChanges();
		}

        public override Task<int> SaveChangesAsync(CancellationToken cancellationToken = default (CancellationToken))
        {
            return SaveChangesAsync(true, cancellationToken);
        }
        
		public override async Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default(CancellationToken))
		{
			AddTimestamps();
            try
            {
                var result = await base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
                return result;
            }
            catch (DbUpdateException ex)
            {
                if (ex.InnerException != null) throw ex.InnerException;
                throw;
            }
        }

		public override int SaveChanges(bool acceptAllChangesOnSuccess)
		{
			AddTimestamps();
			return base.SaveChanges(acceptAllChangesOnSuccess);
		}

		private void AddTimestamps()
        {
			var entities = ChangeTracker.Entries().Where(x =>  x.Entity is DexihBaseEntity && (x.State == EntityState.Added || x.State == EntityState.Modified));

			foreach (var entity in entities)
			{
				if (entity.State == EntityState.Added)
				{
					((DexihBaseEntity)entity.Entity).CreateDate = DateTime.UtcNow;
				}
				((DexihBaseEntity)entity.Entity).UpdateDate = DateTime.UtcNow;
            }
		}
        
        private void SetDexihBaseEntity<T>(EntityTypeBuilder<T> entity) where T:DexihBaseEntity
        {
            entity.Property(e => e.CreateDate).HasColumnName("create_date");
            entity.Property(e => e.UpdateDate).HasColumnName("update_date");
            entity.Property(e => e.IsValid).HasColumnName("is_valid");
        }

        private void SetDexihHubEntity<T>(EntityTypeBuilder<T> entity) where T:DexihHubEntity
        {
            entity.Property(e => e.HubKey).IsRequired().HasColumnName("hub_key");
            SetDexihBaseEntity(entity);
        }
        
        private void SetDexihHubKeyEntity<T>(EntityTypeBuilder<T> entity, string keyName = "key") where T:DexihHubKeyEntity
        {
            SetDexihHubEntity(entity);
            entity.Property(e => e.Key).HasColumnName(keyName);
            SetDexihBaseEntity(entity);
        }
        
        private void SetDexihHubNamedEntity<T>(EntityTypeBuilder<T> entity, string keyName = "key") where T:DexihHubNamedEntity
        {
            SetDexihHubKeyEntity(entity, keyName);
            entity.Property(e => e.Key).HasColumnName(keyName);
            entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(250);
            entity.Property(e => e.Description).HasColumnName("description");

            SetDexihBaseEntity(entity);
        }

        private void SetInputParameterBase<T>(EntityTypeBuilder<T> entity, string keyName = "key") where T:InputParameterBase
        {
            SetDexihHubNamedEntity(entity, keyName);

            entity.Property(e => e.Value).HasColumnName("value").HasMaxLength(500).HasJsonConversion();
            entity.Property(e => e.ListOfValuesKey).HasColumnName("list_of_values_key");
            entity.Property(e => e.AllowUserSelect).HasColumnName("allow_user_select");
            entity.Property(e => e.ValueDesc).HasColumnName("value_desc").HasMaxLength(500).HasJsonConversion();
            entity.Property(e => e.Rank).HasColumnName("rank");
        }
        
        private void SetDexihParameterBase<T>(EntityTypeBuilder<T> entity, string keyName = "key") where T:DexihParameterBase
        {
            SetDexihHubKeyEntity(entity, keyName);
            
            entity.Property(e => e.Name).HasColumnName("parameter_name");
            entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(20)
                .HasConversion(new EnumToStringConverter<ETypeCode>());
            entity.Property(e => e.AllowNull).HasColumnName("allow_null");
            entity.Property(e => e.IsGeneric).HasColumnName("is_generic");
            entity.Property(e => e.Direction).HasColumnName("direction").HasMaxLength(20)
                .HasConversion(new EnumToStringConverter<EParameterDirection>());

            entity.Property(e => e.Position).HasColumnName("position");
            entity.Property(e => e.Rank).HasColumnName("rank");
        }

        private void SetDexihFunctionParameterBase<T>(EntityTypeBuilder<T> entity, string keyName = "key")
            where T : DexihFunctionParameterBase
        {
            SetDexihParameterBase(entity, keyName);
            
            entity.Property(e => e.Name).HasColumnName("parameter_name");
            entity.Property(e => e.DatalinkColumnKey).HasColumnName("datalink_column_key");
            entity.Property(e => e.ListOfValuesString).HasColumnName("list_of_values").HasMaxLength(8000);
            entity.Property(e => e.Value).HasColumnName("value").HasMaxLength(50);
        }
        
        private void SetDexihColumnBase<T>(EntityTypeBuilder<T> entity, string keyName = "key") where T:DexihColumnBase
        {
            SetDexihHubNamedEntity(entity, keyName);
            
                entity.Property(e => e.Key).HasColumnName(keyName);
                entity.Property(e => e.AllowDbNull).HasColumnName("allow_db_null");
                entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<ETypeCode>());
                entity.Property(e => e.DeltaType).HasColumnName("delta_type").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EDeltaType>());
                entity.Property(e => e.IsIncrementalUpdate).HasColumnName("is_incremental_update");
                entity.Property(e => e.IsMandatory).HasColumnName("is_mandatory");
                entity.Property(e => e.IsUnique).HasColumnName("is_unique");
                entity.Property(e => e.LogicalName).HasColumnName("logical_name").HasMaxLength(250);   
                entity.Property(e => e.ColumnGroup).HasColumnName("column_group").HasMaxLength(250);
                entity.Property(e => e.DefaultValue).HasColumnName("default_value").HasMaxLength(1024);
                entity.Property(e => e.IsUnicode).HasColumnName("is_unicode");
                entity.Property(e => e.MaxLength).HasColumnName("max_length");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.Precision).HasColumnName("precision");
                entity.Property(e => e.Scale).HasColumnName("scale");
                entity.Property(e => e.SecurityFlag).HasColumnName("security_flag").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<ESecurityFlag>());
                entity.Property(e => e.IsInput).HasColumnName("is_input");
                entity.Property(e => e.Rank).HasColumnName("rank");
                entity.Property(e => e.Format).HasColumnName("format");
        }
        
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            //map the standard identity tables to the repository.
            modelBuilder.Entity<ApplicationUser>(entity =>
            {
                entity.ToTable("dexih_Users");
                entity.Property(e => e.Id).HasMaxLength(36);
            });
            
            modelBuilder.Entity<IdentityRole>(entity =>
            {
                entity.ToTable("dexih_Roles");
                entity.Property(e => e.Id).HasMaxLength(36);
            });
            modelBuilder.Entity<IdentityRoleClaim<string>>(entity =>
            {
                entity.ToTable("dexih_RoleClaims");
                entity.Property(e => e.RoleId).HasMaxLength(36);
            });
            modelBuilder.Entity<IdentityUserRole<string>>(entity =>
            {
                entity.ToTable("dexih_UserRoles");
                entity.Property(e => e.UserId).HasMaxLength(36);
                entity.Property(e => e.RoleId).HasMaxLength(36);
            });
            modelBuilder.Entity<IdentityUserLogin<string>>(entity =>
            {
                entity.ToTable("dexih_UserLogins");
                entity.Property(e => e.UserId).HasMaxLength(36);
            });
            modelBuilder.Entity<IdentityUserClaim<string>>(entity =>
            {
                entity.ToTable("dexih_UserClaims");
                entity.Property(e => e.UserId).HasMaxLength(36);
            });
            modelBuilder.Entity<IdentityUserToken<string>>(entity =>
            {
                entity.ToTable("dexih_UserTokens");
                entity.Property(e => e.UserId).HasMaxLength(36);
            });
            
            modelBuilder.Entity<DexihApi>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_api");

                entity.ToTable("dexih_apis");

                SetDexihHubNamedEntity(entity, "api_key");

                entity.Property(e => e.SourceDatalinkKey).HasColumnName("source_datalink_key");
                entity.Property(e => e.SourceTableKey).HasColumnName("source_table_key");
                entity.Property(e => e.SourceType).IsRequired().HasColumnName("source_type").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<ESourceType>());
                entity.Property(e => e.LogDirectory).HasColumnName("log_directory").HasMaxLength(250);
                entity.Property(e => e.AutoStart).HasColumnName("auto_start");
                entity.Property(e => e.CacheQueries).IsRequired().HasColumnName("cache_queries");
                entity.Property(e => e.CacheResetInterval).HasColumnName("cache_reset_interval");
                entity.Property(e => e.SelectQuery).HasColumnName("select_query").HasJsonConversion();
                entity.Property(e => e.IsShared).HasColumnName("is_shared");
                
                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihApis)
                    .HasForeignKey(d => d.HubKey);

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihApiParameter>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_api_parameters");

                entity.ToTable("dexih_api_parameter");

                SetInputParameterBase(entity, "api_parameter_key");
                entity.Property(e => e.ApiKey).HasColumnName("api_key");

                entity.HasOne(d => d.Api)
                    .WithMany(p => p.Parameters)
                    .HasForeignKey(d => d.ApiKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_api_parameters");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);

            });
                        
            modelBuilder.Entity<DexihColumnValidation>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_column_validation");

                entity.ToTable("dexih_column_validation");

                SetDexihHubNamedEntity(entity, "column_validation_key");

                entity.Property(e => e.AllowDbNull).HasColumnName("allow_db_null");

                entity.Property(e => e.CleanAction).HasColumnName("clean_action").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<ECleanAction>());
                
                entity.Property(e => e.InvalidAction).HasColumnName("invalid_action").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EInvalidAction>());
                
                entity.Property(e => e.DataType).HasColumnName("datatype").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<ETypeCode>());
                
                entity.Property(e => e.CleanValue).HasColumnName("clean_value").HasMaxLength(250);

                entity.Property(e => e.ListOfValuesString).HasColumnName("list_of_values").HasMaxLength(8000);

                entity.Property(e => e.ListOfNotValuesString).HasColumnName("list_of_not_values").HasMaxLength(8000);
                
                entity.Property(e => e.LookupColumnKey).HasColumnName("lookup_column_key");
                entity.Property(e => e.LookupIsValid).HasColumnName("lookup_is_valid");
                entity.Property(e => e.LookupMultipleRecords).HasColumnName("lookup_multiple_records");

                entity.Property(e => e.MinLength).HasColumnName("min_length");
                entity.Property(e => e.MaxLength).HasColumnName("max_length");

                entity.Property(e => e.MinValue).HasColumnName("min_value");
                entity.Property(e => e.MaxValue).HasColumnName("max_value");

                entity.Property(e => e.PatternMatch).HasColumnName("pattern_match").HasMaxLength(250);
                entity.Property(e => e.RegexMatch).HasColumnName("regex_match").HasMaxLength(250);

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihColumnValidations)
                    .HasForeignKey(d => d.HubKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_column_validation_dexih_hubs");
                
                entity.HasOne(d => d.LookupColumn)
                    .WithMany(p => p.DexihColumnValidationLookupColumn)
                    .HasForeignKey(d => d.LookupColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_column_validation_lookup_column");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);

            });

            modelBuilder.Entity<DexihConnection>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_connections");

                entity.ToTable("dexih_connections");
                
                SetDexihHubNamedEntity(entity, "connection_key");
                
                entity.Property(e => e.Purpose).HasColumnName("purpose").HasMaxLength(10)
                    .HasConversion(new EnumToStringConverter<EConnectionPurpose>());

                entity.Property(e => e.ConnectionAssemblyName).HasColumnName("connection_assembly_name");
                entity.Property(e => e.ConnectionClassName).HasColumnName("connection_class_name");

                entity.Property(e => e.UseConnectionString).HasColumnName("use_connection_string");
                entity.Property(e => e.UseConnectionStringVariable).HasColumnName("use_connection_string_var");
                entity.Property(e => e.ConnectionString).HasColumnName("connection_string").HasMaxLength(1000);

                entity.Property(e => e.Server).HasColumnName("server").HasMaxLength(250);

                entity.Property(e => e.DefaultDatabase).HasColumnName("default_database").HasMaxLength(50);
				entity.Property(e => e.EmbedTableKey).HasColumnName("embed_tablekey");
                entity.Property(e => e.Filename).HasColumnName("filename").HasMaxLength(1000);

				entity.Property(e => e.UseWindowsAuth).HasColumnName("use_windows_auth");
                entity.Property(e => e.Username).HasColumnName("username").HasMaxLength(250);
                entity.Property(e => e.Password).HasColumnName("password").HasMaxLength(250);
                entity.Property(e => e.UsePasswordVariable).HasColumnName("use_password_var");

                entity.Property(e => e.ConnectionTimeout).HasColumnName("connection_timeout");
                entity.Property(e => e.CommandTimeout).HasColumnName("command_timeout");

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihConnections)
                    .HasForeignKey(d => d.HubKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_connections_dexih_hubs");
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihCustomFunction>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_custom_functions");

                entity.ToTable("dexih_custom_functions");

                SetDexihHubNamedEntity(entity, "custom_function_key");

                entity.Property(e => e.MethodCode).HasColumnName("method_code").HasMaxLength(8000);
                entity.Property(e => e.ResultCode).HasColumnName("result_code").HasMaxLength(8000);

                entity.Property(e => e.IsGeneric).HasColumnName("is_generic");
                entity.Property(e => e.GenericTypeDefault).HasColumnName("generic_type_default").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<ETypeCode>());

                entity.Property(e => e.FunctionType).HasColumnName("function_type").HasMaxLength(30)
                    .HasConversion(new EnumToStringConverter<EFunctionType>());
                
                entity.Property(e => e.ReturnType).HasColumnName("return_type").HasMaxLength(30)
                    .HasConversion(new EnumToStringConverter<ETypeCode>());
                
                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihCustomFunctions)
                    .HasForeignKey(d => d.HubKey);
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihCustomFunctionParameter>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_cust_function_parameters");

                entity.ToTable("dexih_custom_function_parameters");

                SetDexihParameterBase(entity, "custom_function_parameter_key");

                entity.Property(e => e.CustomFunctionKey).HasColumnName("custom_function_key");

                entity.HasOne(d => d.CustomFunction)
                    .WithMany(p => p.DexihCustomFunctionParameters)
                    .HasForeignKey(d => d.CustomFunctionKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_customfunction_customfunction_parameter");
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDashboardItem>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_dashboard_items");

                entity.ToTable("dexih_dashboard_item");

                SetDexihHubNamedEntity(entity, "dashboard_item_key");
                
                entity.Property(e => e.DashboardKey).HasColumnName("dashboard_key");
                
                entity.Property(e => e.X).HasColumnName("x");
                entity.Property(e => e.Y).HasColumnName("y");
                entity.Property(e => e.Cols).HasColumnName("cols");
                entity.Property(e => e.Rows).HasColumnName("rows");
                entity.Property(e => e.Header).HasColumnName("header");
                entity.Property(e => e.Scrollable).HasColumnName("scrollable");
                entity.Property(e => e.ViewKey).HasColumnName("view_key");
                
                entity.HasOne(d => d.Dashboard)
                    .WithMany(p => p.DexihDashboardItems)
                    .HasForeignKey(d => d.DashboardKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_dashboard_items_dexih_dashboards");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihDashboardParameter>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_dashboard_parameters");

                entity.ToTable("dexih_dashboard_parameter");

                SetInputParameterBase(entity, "dashboard_parameter_key");
                entity.Property(e => e.DashboardKey).HasColumnName("dashboard_key");

                entity.HasOne(d => d.Dashboard)
                    .WithMany(p => p.Parameters)
                    .HasForeignKey(d => d.DashboardKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_dashboard_parameters_dexih_dashboards");

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDashboardItemParameter>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_dashboard_item_parameters");

                entity.ToTable("dexih_dashboard_item_parameter");

                SetInputParameterBase(entity, "dashboard_item_parameter_key");

                entity.Property(e => e.DashboardItemKey).HasColumnName("dashboard_item_key");
                
                entity.HasOne(d => d.DashboardItem)
                    .WithMany(p => p.Parameters)
                    .HasForeignKey(d => d.DashboardItemKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_dashboard_item_parameter_dexih_dash_item");

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDashboard>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_dashboards");

                entity.ToTable("dexih_dashboards");

                SetDexihHubNamedEntity(entity, "dashboard_key");

                entity.Property(e => e.MinCols).HasColumnName("min_cols");
                entity.Property(e => e.MaxCols).HasColumnName("max_cols");
                entity.Property(e => e.MinRows).HasColumnName("min_rows");
                entity.Property(e => e.MaxRows).HasColumnName("max_rows");
                entity.Property(e => e.AutoRefresh).HasColumnName("auto_refresh");

				entity.Property(e => e.IsShared).HasColumnName("is_shared");
                
                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihDashboards)
                    .HasForeignKey(d => d.HubKey);

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihDatajob>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datajob");

                entity.ToTable("dexih_datajobs");

                SetDexihHubNamedEntity(entity, "datajob_key");

                entity.Property(e => e.FailAction).HasColumnName("fail_action").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<EFailAction>());

                entity.Property(e => e.AuditConnectionKey).HasColumnName("audit_connection_key");
                // entity.Property(e => e.ExternalTrigger).HasColumnName("external_trigger");
                entity.Property(e => e.FileWatch).HasColumnName("file_watch");
                entity.Property(e => e.AutoStart).HasColumnName("auto_start");

                entity.Property(e => e.AlertLevel).HasColumnName("alert_level").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<EAlertLevel>());

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihDatajobs)
                    .HasForeignKey(d => d.HubKey)
                    .HasConstraintName("FK_dexih_datajobs_dexih_hubs");

                entity.HasOne(d => d.AuditConnection)
                    .WithMany(p => p.DexihDatajobAuditConnections)
                    .HasForeignKey(d => d.AuditConnectionKey)
                    .HasConstraintName("FK_dexih_datajobs_audit_connection");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDatajobParameter>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_datajob_parameters");

                entity.ToTable("dexih_datajob_parameter");

                SetInputParameterBase(entity, "datajob_parameter_key");

                entity.Property(e => e.DatajobKey).HasColumnName("datajob_key");
                
                entity.HasOne(d => d.Datajob)
                    .WithMany(p => p.Parameters)
                    .HasForeignKey(d => d.DatajobKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datajob_parameters_dexih_datajob_parameters");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihDatalinkColumn>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink_columns");

                entity.ToTable("dexih_datalink_columns");

                SetDexihColumnBase(entity, "datalink_column_key");
                entity.Property(e => e.DatalinkTableKey).HasColumnName("datalink_table_key");
                entity.Property(e => e.ParentDatalinkColumnKey).HasColumnName("parent_datalink_column_key");

                entity.HasOne(d => d.DatalinkTable)
                    .WithMany(p => p.DexihDatalinkColumns)
                    .HasForeignKey(d => d.DatalinkTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalinktable_columns_dexih_datalinktables");
                
                entity.HasOne(d => d.ParentColumn)
                    .WithMany(p => p.ChildColumns)
                    .HasForeignKey(d => d.ParentDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_table_child_columns");

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihDatalinkTable>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink_table");

                entity.ToTable("dexih_datalink_tables");

                SetDexihHubNamedEntity(entity, "datalink_table_key");
                entity.Property(e => e.SourceTableKey).HasColumnName("source_table_key");
                entity.Property(e => e.SourceDatalinkKey).HasColumnName("source_datalink_key");
                entity.Property(e => e.SourceType).HasColumnName("source_type").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<ESourceType>());

                entity.Property(e => e.RowsStartAt).HasColumnName("rows_start_at");
                entity.Property(e => e.RowsEndAt).HasColumnName("rows_end_at");
                entity.Property(e => e.RowsIncrement).HasColumnName("rows_increment");

                entity.Property(e => e.DisablePushDown).HasColumnName("disable_push_down");
                entity.Property(e => e.DisableVersioning).HasColumnName("disable_versioning");

                entity.HasOne(d => d.SourceTable)
                    .WithMany(p => p.DexihDatalinkTables)
                    .HasForeignKey(d => d.SourceTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_source_tables");

                entity.HasOne(d => d.SourceDatalink)
                    .WithMany(p => p.DexihDatalinkTables)
                    .HasForeignKey(d => d.SourceDatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_source_datalinks");

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihDatalinkDependency>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_datalink_dependencies");

                entity.ToTable("dexih_datalink_dependencies");

                SetDexihHubKeyEntity(entity, "datalink_dependency_key");
                entity.Property(e => e.DatalinkStepKey).HasColumnName("datalink_step_key");
                entity.Property(e => e.DependentDatalinkStepKey).HasColumnName("dependent_datalink_step_key");
                
                entity.HasOne(d => d.DependentDatalinkStep)
                    .WithMany(p => p.DexihDatalinkDependentSteps)
                    .HasForeignKey(d => d.DependentDatalinkStepKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_deps_dl_dep");
                
                entity.HasOne(d => d.DatalinkStep)
                    .WithMany(p => p.DexihDatalinkDependencies)
                    .HasForeignKey(d => d.DatalinkStepKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_deps_dl_step");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDatalinkParameter>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_datalink_parameters");

                entity.ToTable("dexih_datalink_parameter");

                SetInputParameterBase(entity, "datalink_parameter_key");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
                
                entity.HasOne(d => d.Datalink)
                    .WithMany(p => p.Parameters)
                    .HasForeignKey(d => d.DatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_parameters_dexih_datalinks");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihDatalinkProfile>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_datalink_profile_rules");

                entity.ToTable("dexih_datalink_profiles");

                SetDexihHubKeyEntity(entity, "datalink_profile_key");

                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
				entity.Property(e => e.DetailedResults).HasColumnName("detailed_results");
                entity.Property(e => e.FunctionClassName).HasColumnName("function_class_name");
                entity.Property(e => e.FunctionMethodName).HasColumnName("function_method_name");
                entity.Property(e => e.FunctionAssemblyName).HasColumnName("function_assembly_name");
                
                entity.HasOne(d => d.Datalink)
                    .WithMany(p => p.DexihDatalinkProfiles)
                    .HasForeignKey(d => d.DatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_profiles_dexih_datalinks");

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

 

            modelBuilder.Entity<DexihDatalinkStep>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_datajobs_datalinks");

                entity.ToTable("dexih_datalink_steps");

                SetDexihHubNamedEntity(entity, "datalink_step_key");

                entity.Property(e => e.DatajobKey).HasColumnName("datajob_key");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
                entity.Property(e => e.Position).HasColumnName("position");

                entity.HasOne(d => d.Datajob)
                    .WithMany(p => p.DexihDatalinkSteps)
                    .HasForeignKey(d => d.DatajobKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datajobs_datalinks_dexih_datajobs");

                entity.HasOne(d => d.Datalink)
                    .WithMany(p => p.DexihDatalinkSteps)
                    .HasForeignKey(d => d.DatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_step_dexih_datalinks");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });        
            
            modelBuilder.Entity<DexihDatalinkStepParameter>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_datalink_step_parameters");

                entity.ToTable("dexih_datalink_step_parameter");

                SetInputParameterBase(entity, "datalink_step_parameter_key");
                entity.Property(e => e.DatalinkStepKey).HasColumnName("datalink_step_key");

                entity.HasOne(d => d.DatalinkStep)
                    .WithMany(p => p.Parameters)
                    .HasForeignKey(d => d.DatalinkStepKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_step_parameters_dexih_datalinks_steps");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDatalinkStepColumn>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink_step_columns");

                entity.ToTable("dexih_datalink_step_columns");

                SetDexihColumnBase(entity, "datalink_step_column_key");
                entity.Property(e => e.DatalinkStepKey).HasColumnName("datalink_step_key");

                entity.HasOne(d => d.DatalinkStep)
                    .WithMany(p => p.DexihDatalinkStepColumns)
                    .HasForeignKey(d => d.DatalinkStepKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalinkstep_columns_dexih_datalinksteps");

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDatalinkTarget>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink_target");

                entity.ToTable("dexih_datalink_targets");

                SetDexihHubKeyEntity(entity, "datalink_target_key");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.NodeDatalinkColumnKey).HasColumnName("node_datalink_column_key");
                entity.Property(e => e.TableKey).HasColumnName("table_key");
                
                entity.HasOne(d => d.Datalink)
                    .WithMany(p => p.DexihDatalinkTargets)
                    .HasForeignKey(d => d.DatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_target_datalink");
                
                entity.HasOne(d => d.NodeDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTargetNodeColumn)
                    .HasForeignKey(d => d.NodeDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_target_datalink_node_column");
                
                entity.HasOne(d => d.Table)
                    .WithMany(p => p.DexihTargetTables)
                    .HasForeignKey(d => d.TableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_target_table");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDatalinkTest>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink_tests");

                entity.ToTable("dexih_datalink_tests");

                SetDexihHubNamedEntity(entity, "datalink_test_key");
                entity.Property(e => e.AuditConnectionKey).HasColumnName("audit_connection_key");
                
                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihDatalinkTests)
                    .HasForeignKey(d => d.HubKey);

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDatalinkTestStep>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink_test_steps");

                entity.ToTable("dexih_datalink_test_steps");

                SetDexihHubNamedEntity(entity, "datalink_test_step_key");
                entity.Property(e => e.DatalinkTestKey).HasColumnName("datalink_test_key");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
                
               
                entity.Property(e => e.TargetConnectionKey).HasColumnName("target_connection_key");
                entity.Property(e => e.TargetTableName).HasColumnName("target_table_name").HasMaxLength(50);
                entity.Property(e => e.TargetSchema).HasColumnName("target_schema").HasMaxLength(50);

                entity.Property(e => e.ExpectedConnectionKey).HasColumnName("expected_connection_key");
                entity.Property(e => e.ExpectedTableName).HasColumnName("expected_table_name").HasMaxLength(50);
                entity.Property(e => e.ExpectedSchema).HasColumnName("expected_schema").HasMaxLength(50);

                entity.Property(e => e.ErrorConnectionKey).HasColumnName("error_connection_key");
                entity.Property(e => e.ErrorTableName).HasColumnName("error_table_name").HasMaxLength(50);
                entity.Property(e => e.ErrorSchema).HasColumnName("error_schema").HasMaxLength(50);

                entity.HasOne(d => d.DatalinkTest)
                    .WithMany(p => p.DexihDatalinkTestSteps)
                    .HasForeignKey(d => d.DatalinkTestKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_test_datalink_test_step");

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihDatalinkTestTable>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink_test_tables");

                entity.ToTable("dexih_datalink_test_tables");

                SetDexihHubKeyEntity(entity, "datalink_test_table_key");
                entity.Property(e => e.DatalinkTestStepKey).HasColumnName("datalink_test_step_key");
                
                entity.Property(e => e.TableKey).HasColumnName("table_key");
                entity.Property(e => e.SourceConnectionKey).HasColumnName("source_connection_key");
                entity.Property(e => e.SourceTableName).HasColumnName("source_table_name").HasMaxLength(50);
                entity.Property(e => e.SourceSchema).HasColumnName("source_schema").HasMaxLength(50);

                entity.Property(e => e.TestConnectionKey).HasColumnName("test_connection_key");
                entity.Property(e => e.TestTableName).HasColumnName("test_table_name").HasMaxLength(50);
                entity.Property(e => e.TestSchema).HasColumnName("test_schema").HasMaxLength(50);

                entity.Property(e => e.Action).HasColumnName("action").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<ETestTableAction>());
                
                entity.HasOne(d => d.DatalinkTestStep)
                    .WithMany(p => p.DexihDatalinkTestTables)
                    .HasForeignKey(d => d.DatalinkTestStepKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_test_step_datalink_test_table");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihDatalinkTransformItem>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink_transform_items");
                entity.ToTable("dexih_datalink_transform_items");

                SetDexihHubNamedEntity(entity, "datalink_transform_item_key");
                entity.Property(e => e.DatalinkTransformKey).HasColumnName("datalink_transform_key");

                entity.Property(e => e.FunctionCode).HasColumnName("function_code").HasMaxLength(8000);
                entity.Property(e => e.FunctionResultCode).HasColumnName("function_result_code").HasMaxLength(8000);
                entity.Property(e => e.InvalidAction).HasColumnName("invalid_action").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<EInvalidAction>());

                entity.Property(e => e.JoinDatalinkColumnKey).HasColumnName("join_datalink_column_key");
                entity.Property(e => e.FilterDatalinkColumnKey).HasColumnName("filter_datalink_column_key");
                entity.Property(e => e.SourceValue).HasColumnName("source_value").HasMaxLength(1000);
                entity.Property(e => e.JoinValue).HasColumnName("join_value").HasMaxLength(1000);
                entity.Property(e => e.FilterValue).HasColumnName("filter_value").HasMaxLength(1000);
                entity.Property(e => e.FilterCompare).HasColumnName("filter_compare").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<ECompare>());
                entity.Property(e => e.Aggregate).HasColumnName("aggregate").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EAggregate>());
                entity.Property(e => e.OnError).HasColumnName("on_error").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EErrorAction>());
                entity.Property(e => e.OnNull).HasColumnName("on_null").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EErrorAction>());
                entity.Property(e => e.SortDirection).HasColumnName("sort_direction").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<ESortDirection>());
                entity.Property(e => e.SeriesGrain).HasColumnName("series_grain").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<ESeriesGrain>());
                entity.Property(e => e.SeriesStep).HasColumnName("series_step").HasMaxLength(40);
                entity.Property(e => e.TransformItemType).HasColumnName("transform_item_type").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<ETransformItemType>());

                entity.Property(e => e.NotCondition).HasColumnName("not_condition");
                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.SeriesFill).HasColumnName("series_fill");
                entity.Property(e => e.SeriesStart).HasColumnName("series_start").HasMaxLength(40);
                entity.Property(e => e.SeriesFinish).HasColumnName("series_finish").HasMaxLength(40);
                entity.Property(e => e.SeriesProject).HasColumnName("series_project").HasMaxLength(40);
                entity.Property(e => e.SourceDatalinkColumnKey).HasColumnName("source_datalink_column_key");
                entity.Property(e => e.FunctionClassName).HasColumnName("function_class_name");
                entity.Property(e => e.FunctionMethodName).HasColumnName("function_method_name");
                entity.Property(e => e.FunctionAssemblyName).HasColumnName("function_assembly_name");
                entity.Property(e => e.FunctionCaching).HasColumnName("function_caching").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EFunctionCaching>());

                entity.Property(e => e.IsGeneric).HasColumnName("is_generic");
                entity.Property(e => e.GenericTypeCode).HasColumnName("generic_type_code").HasMaxLength(20)
                .HasConversion(new EnumToStringConverter<ETypeCode>());

                entity.Property(e => e.CustomFunctionKey).HasColumnName("custom_function_key");
                entity.Property(e => e.TargetDatalinkColumnKey).HasColumnName("target_datalink_column_key");
                
                entity.HasOne(d => d.Dt)
                    .WithMany(p => p.DexihDatalinkTransformItems)
                    .HasForeignKey(d => d.DatalinkTransformKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_dexih_datalink_transforms");

                entity.HasOne(d => d.SourceDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformItemsSourceColumn)
                    .HasForeignKey(d => d.SourceDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_source_column");

                entity.HasOne(d => d.TargetDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformItemsTargetColumn)
                    .HasForeignKey(d => d.TargetDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_target_column");

                entity.HasOne(d => d.JoinDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformItemsJoinColumn)
                    .HasForeignKey(d => d.JoinDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_join_column");

                entity.HasOne(d => d.FilterDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformItemsFilterColumn)
                    .HasForeignKey(d => d.FilterDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_filter_column");

                entity.HasOne(d => d.CustomFunction)
                    .WithMany(p => p.DexihDatalinkTransformItemCustomFunction)
                    .HasForeignKey(d => d.CustomFunctionKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transform_items_dexih_custom_functions");

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihDatalinkTransform>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink_transforms");
                entity.ToTable("dexih_datalink_transforms");

                SetDexihHubNamedEntity(entity, "datalink_transform_key");
                entity.Property(e => e.DatalinkKey).HasColumnName("datalink_key");
                entity.Property(e => e.Position).HasColumnName("position");

                entity.Property(e => e.TransformType).HasColumnName("transform_type").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<ETransformType>());

                entity.Property(e => e.TransformClassName).HasColumnName("transform_class_name").HasMaxLength(250);
                entity.Property(e => e.TransformAssemblyName).HasColumnName("transform_assembly_name").HasMaxLength(250);

                entity.Property(e => e.PassThroughColumns).HasColumnName("pass_through_columns");

                entity.Property(e => e.JoinDatalinkTableKey).HasColumnName("join_datalink_table_key");
                
                entity.Property(e => e.NodeDatalinkColumnKey).HasColumnName("node_datalink_column_key");

                entity.Property(e => e.JoinStrategy).HasColumnName("join_strategy").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EJoinStrategy>());

                entity.Property(e => e.JoinDuplicateStrategy).HasColumnName("join_duplicate_strategy").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EDuplicateStrategy>());

                entity.Property(e => e.JoinNotFoundStrategy).HasColumnName("join_not_found_strategy").HasMaxLength(50)
                .HasConversion(new EnumToStringConverter<EJoinNotFoundStrategy>());

                entity.Property(e => e.JoinSortDatalinkColumnKey).HasColumnName("join_sort_column_key");
                
                entity.Property(e => e.MaxInputRows).HasColumnName("max_input_rows");
                entity.Property(e => e.MaxOutputRows).HasColumnName("max_output_rows");
                
                entity.Property(e => e.DataCache).HasColumnName("data_cache");
                entity.Property(e => e.DataCacheConnectionKey).HasColumnName("data_cache_connection_key");
                
                entity.HasOne(d => d.Datalink)
                    .WithMany(p => p.DexihDatalinkTransforms)
                    .HasForeignKey(d => d.DatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transforms_dexih_datalinks");

                entity.HasOne(d => d.JoinDatalinkTable)
                    .WithMany(p => p.DexihDatalinkTransforms)
                    .HasForeignKey(d => d.JoinDatalinkTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transforms_datalink_join_table");

                entity.HasOne(d => d.JoinSortDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformsJoinSortColumn)
                    .HasForeignKey(d => d.JoinSortDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transforms_datalink_join_sort_column");
                
                entity.HasOne(d => d.NodeDatalinkColumn)
                    .WithMany(p => p.DexihDatalinkTransformsNodeColumn)
                    .HasForeignKey(d => d.NodeDatalinkColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_transforms_datalink_node_column");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihDatalink>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_datalink");
                entity.ToTable("dexih_datalinks");

                SetDexihHubNamedEntity(entity, "datalink_key");

                entity.Property(e => e.DatalinkType).HasColumnName("datalink_type").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EDatalinkType>());
                
                entity.Property(e => e.SourceDatalinkTableKey).HasColumnName("source_datalink_table_key");
                // entity.Property(e => e.TargetTableKey).HasColumnName("target_table_key");
                entity.Property(e => e.AuditConnectionKey).HasColumnName("audit_connection_key");

                entity.Property(e => e.MaxRows).HasColumnName("max_rows");
                entity.Property(e => e.AddDefaultRow).HasColumnName("add_default_row");
                entity.Property(e => e.IsQuery).HasColumnName("is_query");
                entity.Property(e => e.RollbackOnFail).HasColumnName("rollback_on_fail");
                entity.Property(e => e.RowsPerCommit).HasColumnName("rows_per_commit");
                entity.Property(e => e.RowsPerProgress).HasColumnName("rows_per_progress");

                entity.Property(e => e.LoadStrategy).HasColumnName("load_strategy").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<TransformWriterTarget.ETransformWriterMethod>());
                
                entity.Property(e => e.UpdateStrategy).HasColumnName("update_strategy").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EUpdateStrategy>());
                
                entity.Property(e => e.ProfileTableName).HasColumnName("profile_table_name");
                entity.Property(e => e.IsShared).HasColumnName("is_shared");

                entity.Property(e => e.AlertLevel).HasColumnName("alert_level").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<EAlertLevel>());

                entity.HasOne(d => d.SourceDatalinkTable)
                    .WithMany(p => p.DexihDatalinkSourceTables)
                    .HasForeignKey(d => d.SourceDatalinkTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_source_tables");

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihDatalinks)
                    .HasForeignKey(d => d.HubKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalinks_dexih_hubs");


                entity.HasOne(d => d.AuditConnection)
                    .WithMany(p => p.DexihDatalinkAuditConnections)
                    .HasForeignKey(d => d.AuditConnectionKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalinks_audit_connection");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);

            });

            modelBuilder.Entity<DexihFileFormat>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_file_format");

                entity.ToTable("dexih_file_formats");

                SetDexihHubNamedEntity(entity, "file_format_key");
                
                entity.Property(e => e.MatchHeaderRecord).HasColumnName("match_header_record");
                entity.Property(e => e.SkipHeaderRows).HasColumnName("skip_header_rows");
                entity.Property(e => e.AllowComments).HasColumnName("allow_comments");
                entity.Property(e => e.BufferSize).HasColumnName("buffer_size");
                entity.Property(e => e.Comment).HasColumnName("comment").HasColumnType("char(1)");
                entity.Property(e => e.Delimiter).HasColumnName("delimiter");
                entity.Property(e => e.DetectColumnCountChanges).HasColumnName("detect_column_count_changes");
                entity.Property(e => e.HasHeaderRecord).HasColumnName("has_header_record");
                entity.Property(e => e.IgnoreHeaderWhiteSpace).HasColumnName("ignore_header_white_space");
                entity.Property(e => e.IgnoreReadingExceptions).HasColumnName("ignore_reading_exceptions");
                entity.Property(e => e.IgnoreQuotes).HasColumnName("ignore_quotes");
                entity.Property(e => e.Quote).HasColumnName("quote").HasColumnType("char(1)");
                entity.Property(e => e.QuoteAllFields).HasColumnName("quote_all_fields");
                entity.Property(e => e.QuoteNoFields).HasColumnName("quote_no_fields");
                entity.Property(e => e.SkipEmptyRecords).HasColumnName("skip_empty_records");
                entity.Property(e => e.TrimFields).HasColumnName("trim_fields");
                entity.Property(e => e.TrimHeaders).HasColumnName("trim_headers");
                entity.Property(e => e.WillThrowOnMissingField).HasColumnName("will_throw_on_missing_field");
                entity.Property(e => e.SetWhiteSpaceCellsToNull).HasColumnName("set_white_space_cells_to_null");
                
				entity.HasOne(d => d.Hub)
					  .WithMany(p => p.DexihFileFormats)
                    .HasForeignKey(d => d.HubKey);

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihFunctionParameter>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_function_parameters");

                entity.ToTable("dexih_function_parameters");

                SetDexihFunctionParameterBase(entity, "function_parameter_key");
                entity.Property(e => e.DatalinkTransformItemKey).HasColumnName("datalink_transform_item_key");

                entity.HasOne(d => d.DtItem)
                    .WithMany(p => p.DexihFunctionParameters)
                    .HasForeignKey(d => d.DatalinkTransformItemKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_function_parameters_dexih_datalink_transform_items");

                entity.HasOne(d => d.DatalinkColumn)
                    .WithMany(p => p.DexihFunctionParameterColumn)
                    .HasForeignKey(d => d.DatalinkColumnKey)
                    .HasConstraintName("FK_dexih_function_parameters_dexih_table_columns");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihFunctionArrayParameter>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_function_array_param_key");

                entity.ToTable("dexih_function_array_parameters");

                SetDexihFunctionParameterBase(entity, "function_parameter_array_key");
 
                entity.Property(e => e.FunctionParameterKey).HasColumnName("function_parameter_key");
 
                entity.HasOne(d => d.FunctionParameter)
                    .WithMany(p => p.ArrayParameters)
                    .HasForeignKey(d => d.FunctionParameterKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_function_array_parameter");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);

            });

            modelBuilder.Entity<DexihRemoteAgent>(entity =>
            {
                entity.HasKey(e => e.RemoteAgentKey).HasName("PK_dexih_remote_agent");

                entity.ToTable("dexih_remote_agents");

                SetDexihBaseEntity(entity);
                entity.Property(e => e.RemoteAgentKey).HasColumnName("remote_agent_key");
                entity.Property(e => e.Name).IsRequired().HasColumnName("name").HasMaxLength(50);

                entity.Property(e => e.AllowExternalConnect).HasColumnName("allow_external_connect");
                entity.Property(e => e.IpAddressesString).HasColumnName("ip_addresses").HasMaxLength(8000);

                entity.Property(e => e.UserId).HasColumnName("user_id").HasMaxLength(36);
                entity.Property(e => e.RemoteAgentId).IsRequired().HasColumnName("remote_agent_id").HasMaxLength(50);
                entity.Property(e => e.RestrictIp).HasColumnName("restrict_ip");
                
                entity.Property(e => e.HashedToken).HasColumnName("hashed_token").HasMaxLength(4000);
                
                entity.Property(e => e.LastLoginDateTime).HasColumnName("last_login_date");
                entity.Property(e => e.LastLoginIpAddress).HasColumnName("last_login_ip");
                
                entity.HasQueryFilter(e => e.IsValid);
            });

            modelBuilder.Entity<DexihRemoteAgentHub>(entity =>
            {
                entity.HasKey(e => e.RemoteAgentHubKey).HasName("PK_dexih_remote_agent_hub");

                entity.ToTable("dexih_remote_agent_hubs");

                SetDexihHubEntity(entity);
                entity.Property(e => e.RemoteAgentHubKey).HasColumnName("remote_agent_hub_key");
                entity.Property(e => e.RemoteAgentKey).HasColumnName("remote_agent_key");
                entity.Property(e => e.IsDefault).HasColumnName("is_default");
                entity.Property(e => e.IsAuthorized).HasColumnName("is_authorized");
                
                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihRemoteAgentHubs)
                    .HasForeignKey(d => d.HubKey);
                
                entity.HasOne(d => d.RemoteAgent)
                    .WithMany(p => p.DexihRemoteAgentHubs)
                    .HasForeignKey(d => d.RemoteAgentKey);

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihHubUser>(entity =>
            {
                entity.HasKey(e => new { e.UserId, e.HubKey }).HasName("PK_UserHub");

                entity.ToTable("dexih_hub_users");

                SetDexihBaseEntity(entity);
                entity.Property(e => e.UserId).HasMaxLength(450);
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Permission).HasColumnName("permission").HasMaxLength(50)
                    .HasConversion(new EnumToStringConverter<EPermission>());
                entity.Property(e => e.ReceiveAlerts).HasColumnName("receive_alerts");


                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihHubUsers)
                    .HasForeignKey(d => d.HubKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_user_hub_dexih_hubs");
                
                // entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihHub>(entity =>
            {
                entity.HasKey(e => e.HubKey)
                    .HasName("PK_dexih_hubs");

                entity.ToTable("dexih_hubs");

                SetDexihBaseEntity(entity);
                
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.Name).HasColumnName("name").HasMaxLength(50);
                entity.Property(e => e.Description).HasColumnName("description").HasMaxLength(500);
                entity.Property(e => e.EncryptionKey).HasColumnName("encryption_key").HasMaxLength(255);
                entity.Property(e => e.SharedAccess).HasColumnName("shared_access").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<ESharedAccess>());
                entity.Property(e => e.TimeZone).HasColumnName("timezone").HasMaxLength(255);

                entity.HasQueryFilter(e => e.IsValid);
            });

            modelBuilder.Entity<DexihHubVariable>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_hub_variable");

                entity.ToTable("dexih_hub_variables");

                SetDexihHubNamedEntity(entity, "hub_variable_key");
                entity.Property(e => e.Value).HasColumnName("value").HasMaxLength(1024);
                entity.Property(e => e.IsEncrypted).HasColumnName("is_encrypted");
                entity.Property(e => e.IsEnvironmentVariable).HasColumnName("is_environment_var");
                
                entity.HasOne(d => d.Hub)
                      .WithMany(p => p.DexihHubVariables)
                      .HasForeignKey(d => d.HubKey);
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihIssue>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_issue");

                entity.ToTable("dexih_issues");
                
                SetDexihHubNamedEntity(entity, "issue_key");

                entity.Property(e => e.Type).HasColumnName("type");
                entity.Property(e => e.Category).HasColumnName("category");
                entity.Property(e => e.Severity).HasColumnName("severity");
                entity.Property(e => e.Link).HasColumnName("link").HasMaxLength(250);
                entity.Property(e => e.Data).HasColumnName("data").IsFixedLength(false);
                entity.Property(e => e.GitHubLink).HasColumnName("github_link").HasMaxLength(250);
                entity.Property(e => e.IsPrivate).HasColumnName("is_private");
                entity.Property(e => e.HubKey).HasColumnName("hub_key");
                entity.Property(e => e.UserId).HasColumnName("user_id").HasMaxLength(36);
                entity.Property(e => e.IssueStatus).HasColumnName("issue_status");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihIssueComment>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_issue_comment");

                entity.ToTable("dexih_issue_comments");

                SetDexihBaseEntity(entity);
                entity.Property(e => e.IssueKey).HasColumnName("issue_key");
                entity.Property(e => e.Key).HasColumnName("issue_comment_key");
                entity.Property(e => e.Comment).HasColumnName("comment").IsFixedLength(false);
                entity.Property(e => e.UserId).HasColumnName("user_id").HasMaxLength(36);
                
                entity.HasOne(d => d.Issue)
                    .WithMany(p => p.DexihIssueComments)
                    .HasForeignKey(d => d.IssueKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_issue_comments");
                
                entity.HasQueryFilter(e => e.IsValid);
            });
            
            modelBuilder.Entity<DexihTableColumn>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_table_columns");

                entity.ToTable("dexih_table_columns");

                SetDexihColumnBase(entity, "column_key");
                entity.Property(e => e.ColumnValidationKey).HasColumnName("column_validation_key");
                entity.Property(e => e.ParentColumnKey).HasColumnName("parent_column_key");
                entity.Property(e => e.TableKey).HasColumnName("table_key");

                entity.HasOne(d => d.Table)
                    .WithMany(p => p.DexihTableColumns)
                    .HasForeignKey(d => d.TableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_table_columns_dexih_tables");

                entity.HasOne(d => d.ColumnValidation)
                    .WithMany(p => p.DexihColumnValidationColumn)
                    .HasForeignKey(d => d.ColumnValidationKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_table_columns_dexih_column_validations");
                
                entity.HasOne(d => d.ParentColumn)
                    .WithMany(p => p.ChildColumns)
                    .HasForeignKey(d => d.ParentColumnKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_table_child_columns");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);

            });

            modelBuilder.Entity<DexihTableIndex>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_table_indexes");
                entity.ToTable("dexih_table_indexes");

                SetDexihHubNamedEntity(entity, "table_index_key");
                
                entity.Property(e => e.TableKey).HasColumnName("table_key");
                
                entity.HasOne(d => d.Table)
                    .WithMany(p => p.DexihTableIndexes)
                    .HasForeignKey(d => d.TableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_table_index_dexih_tables");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihTableIndexColumn>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_table_index_columns");
                entity.ToTable("dexih_table_index_columns");

                SetDexihHubKeyEntity(entity, "table_index_column_key");
                entity.Property(e => e.ColumnKey).HasColumnName("column_key");
                entity.Property(e => e.Direction).HasColumnName("direction").HasMaxLength(10)
                    .HasConversion(new EnumToStringConverter<ESortDirection>());
                entity.Property(e => e.TableIndexKey).HasColumnName("table_index_key");

                entity.HasOne(d => d.TableIndex)
                    .WithMany(p => p.Columns)
                    .HasForeignKey(d => d.TableIndexKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_table_index_columns");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihTable>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_tables");

                entity.ToTable("dexih_tables");

                SetDexihHubNamedEntity(entity, "table_key");
                entity.Property(e => e.ConnectionKey).HasColumnName("connection_key");

                entity.Property(e => e.Schema).HasColumnName("schema").HasMaxLength(250);
                entity.Property(e => e.LogicalName).HasColumnName("logical_name").HasMaxLength(250);
                entity.Property(e => e.BaseTableName).HasColumnName("base_table_name").HasMaxLength(250);
                entity.Property(e => e.RejectedTableName).HasColumnName("rejected_table_name").HasMaxLength(250);
                
                entity.Property(e => e.TableType).HasColumnName("table_type").HasMaxLength(10)
                    .HasConversion(new EnumToStringConverter<Table.ETableType>());

                // entity.Property(e => e.UseQuery).HasColumnName("use_query");
                entity.Property(e => e.QueryString).HasColumnName("query_string");

                entity.Property(e => e.FileFormatKey).HasColumnName("file_format_key");
                entity.Property(e => e.AutoManageFiles).HasColumnName("auto_manage_files");
                entity.Property(e => e.UseCustomFilePaths).HasColumnName("use_custom_file_paths");
                entity.Property(e => e.FileMatchPattern).HasColumnName("file_match_pattern").HasMaxLength(255);
                entity.Property(e => e.FileRootPath).HasColumnName("file_root_path").HasMaxLength(1000);
                entity.Property(e => e.FileIncomingPath).HasColumnName("file_incoming_path").HasMaxLength(1000);
                entity.Property(e => e.FileOutgoingPath).HasColumnName("file_outgoing_path").HasMaxLength(1000);
                entity.Property(e => e.FileProcessedPath).HasColumnName("file_processed_path").HasMaxLength(1000);
                entity.Property(e => e.FileRejectedPath).HasColumnName("file_rejected_path").HasMaxLength(1000);
                entity.Property(e => e.SortColumnKeysString).HasColumnName("sort_column_keys").HasMaxLength(500);

                entity.Property(e => e.RestfulUri).HasColumnName("restful_uri").HasMaxLength(2000);
                entity.Property(e => e.RowPath).HasColumnName("row_path").HasMaxLength(2000);
                entity.Property(e => e.FormatType).HasColumnName("format_type").HasMaxLength(10)
                    .HasConversion(new EnumToStringConverter<ETypeCode>());

                entity.Property(e => e.IsVersioned).HasColumnName("is_versioned");
                entity.Property(e => e.SourceConnectionName).HasColumnName("source_connection_name").HasMaxLength(250);
				entity.Property(e => e.IsShared).HasColumnName("is_shared");

                entity.HasOne(d => d.Connection)
                    .WithMany(p => p.DexihTables)
                    .HasForeignKey(d => d.ConnectionKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_tables_dexih_connections");

                entity.HasOne(d => d.FileFormat)
                    .WithMany(p => p.DexihTables)
                    .HasForeignKey(d => d.FileFormatKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_tables_dexih_file_format");
                
                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihTables)
                    .HasForeignKey(d => d.HubKey);

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihTag>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_tags");

                entity.ToTable("dexih_tags");

                SetDexihHubNamedEntity(entity, "tag_key");
                entity.Property(e => e.Color).HasColumnName("color").HasMaxLength(20);

				entity.HasOne(d => d.Hub)
					  .WithMany(p => p.DexihTags)
                    .HasForeignKey(d => d.HubKey);

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihTagObject>(entity =>
            {
                entity.HasKey(e => new {e.TagKey, e.ObjectKey, e.ObjectType})
                    .HasName("PK_dexih_tag_objects");

                entity.ToTable("dexih_tag_objects");

                SetDexihHubEntity(entity);
                entity.Property(e => e.TagKey).HasColumnName("tag_key");
                entity.Property(e => e.ObjectKey).HasColumnName("object_key");
                entity.Property(e => e.ObjectType).HasColumnName("object_type");

                entity.HasOne(d => d.DexihTag)
                    .WithMany(p => p.DexihTagObjects)
                    .HasForeignKey(d => d.TagKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_tagobject_tags");
                
                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihTagObjects)
                    .HasForeignKey(d => d.HubKey);
                
                // entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihTrigger>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_triggers");

                entity.ToTable("dexih_triggers");

                SetDexihHubKeyEntity(entity, "trigger_key");
                entity.Property(e => e.DatajobKey).HasColumnName("datajob_key");

                entity.Property(e => e.Position).HasColumnName("position");
                entity.Property(e => e.StartDate).HasColumnName("start_date");
                entity.Property(e => e.StartTime).HasColumnName("start_time");
                entity.Property(e => e.EndTime).HasColumnName("end_time");
                entity.Property(e => e.IntervalTime).HasColumnName("interval");
                entity.Property(e => e.DaysOfWeekString).HasColumnName("days_of_week").HasMaxLength(200);
                entity.Property(e => e.MaxRecurs).HasColumnName("max_recurrs");
                entity.Property(e => e.CronExpression).HasColumnName("cron_expression").HasMaxLength(500);

                entity.HasOne(d => d.Datajob)
                    .WithMany(p => p.DexihTriggers)
                    .HasForeignKey(d => d.DatajobKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_triggers_dexih_datajobs");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihView>(entity =>
            {
                entity.HasKey(e => e.Key).HasName("PK_dexih_view");

                entity.ToTable("dexih_views");

                SetDexihHubNamedEntity(entity, "view_key");
                entity.Property(e => e.ViewType).IsRequired().HasColumnName("view_type").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<EViewType>());
                entity.Property(e => e.SourceType).IsRequired().HasColumnName("source_type").HasMaxLength(20)
                    .HasConversion(new EnumToStringConverter<EDataObjectType>());
                entity.Property(e => e.SourceDatalinkKey).HasColumnName("source_datalink_key");
                entity.Property(e => e.SourceTableKey).HasColumnName("source_table_key");

                entity.Property(e => e.SelectQuery).HasColumnName("select_query").HasJsonConversion();
                entity.Property(e => e.ChartConfig).HasColumnName("chart_config").HasJsonConversion();
                entity.Property(e => e.AnimateConfig).HasColumnName("animation_config").HasJsonConversion();
                entity.Property(e => e.InputValues).HasColumnName("input_values").HasJsonConversion();

                entity.Property(e => e.AutoRefresh).HasColumnName("auto_refresh");
                entity.Property(e => e.IsShared).HasColumnName("is_shared");

                entity.HasOne(d => d.SourceDatalink)
                    .WithMany(p => p.DexihViews)
                    .HasForeignKey(d => d.SourceDatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_datalink_view_dexih_datalinks");

                entity.HasOne(d => d.SourceTable)
                    .WithMany(p => p.DexihViews)
                    .HasForeignKey(d => d.SourceTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_table_view_dexih_tables");
                
                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihViews)
                    .HasForeignKey(d => d.HubKey);

                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });

            modelBuilder.Entity<DexihViewParameter>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_view_parameters");

                entity.ToTable("dexih_view_parameter");

                SetInputParameterBase(entity, "view_parameter_key");
                entity.Property(e => e.ViewKey).HasColumnName("view_key");
                entity.Property(e => e.DatalinkParameterKey).HasColumnName("datalink_parameter_key");
                
                entity.HasOne(d => d.View)
                    .WithMany(p => p.Parameters)
                    .HasForeignKey(d => d.ViewKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_views_parameters");
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
            
            modelBuilder.Entity<DexihListOfValues>(entity =>
            {
                entity.HasKey(e => e.Key)
                    .HasName("PK_dexih_list_of_values");

                entity.ToTable("dexih_list_of_values");
                
                SetDexihHubNamedEntity(entity, "list_of_values_key");
                
                entity.Property(e => e.SourceType).HasColumnName("source_type");
                entity.Property(e => e.SourceTableKey).HasColumnName("source_table_key");
                entity.Property(e => e.SourceDatalinkKey).HasColumnName("source_datalink_key");

                entity.Property(e => e.SelectQuery).HasColumnName("select_query").HasJsonConversion();

                entity.Property(e => e.KeyColumn).HasColumnName("key_column").HasMaxLength(50);
                entity.Property(e => e.NameColumn).HasColumnName("name_column").HasMaxLength(50);
                entity.Property(e => e.DescriptionColumn).HasColumnName("desc_column").HasMaxLength(50);
                entity.Property(e => e.Cache).HasColumnName("cache");
                entity.Property(e => e.CacheSeconds).HasColumnName("cache_seconds");

                entity.Property(e => e.StaticData).HasColumnName("static_data").HasJsonConversion();

                entity.HasOne(d => d.SourceDatalink)
                    .WithMany(p => p.DexihListOfValues)
                    .HasForeignKey(d => d.SourceDatalinkKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_lov_dexih_datalinks");

                entity.HasOne(d => d.SourceTable)
                    .WithMany(p => p.DexihListOfValues)
                    .HasForeignKey(d => d.SourceTableKey)
                    .OnDelete(DeleteBehavior.Restrict)
                    .HasConstraintName("FK_dexih_lov_dexih_tables");

                entity.HasOne(d => d.Hub)
                    .WithMany(p => p.DexihListOfValues)
                    .HasForeignKey(d => d.HubKey);
                
                entity.HasQueryFilter(e => e.IsValid);
                // entity.HasQueryFilter(e => e.HubKey == HubKey);
            });
        }
        
        public DbSet<DexihApi> DexihApis { get; set; }
        public DbSet<DexihApiParameter> DexihApiParameters { get; set; }
        public DbSet<DexihColumnValidation> DexihColumnValidations { get; set; }
        public DbSet<DexihConnection> DexihConnections { get; set; }
	    public DbSet<DexihCustomFunction> DexihCustomFunctions { get; set; }
        public DbSet<DexihCustomFunctionParameter> DexihCustomFunctionParameters { get; set; }
        public DbSet<DexihDashboard> DexihDashboards { get; set; }
        public DbSet<DexihDashboardItem> DexihDashboardItems { get; set; }
        public DbSet<DexihDashboardParameter> DexihDashboardParameters { get; set; }
        public DbSet<DexihDashboardItemParameter> DexihDashboardItemParameters { get; set; }
        public DbSet<DexihDatajob> DexihDatajobs { get; set; }
        public DbSet<DexihDatajobParameter> DexihDatajobParameters { get; set; }
        public DbSet<DexihDatalinkDependency> DexihDatalinkDependencies { get; set; }
        public DbSet<DexihDatalinkParameter> DexihDatalinkParameters { get; set; }
        public DbSet<DexihDatalinkTable> DexihDatalinkTables { get; set; }
        public DbSet<DexihDatalinkColumn> DexihDatalinkColumns { get; set; }
        public DbSet<DexihDatalinkProfile> DexihDatalinkProfiles { get; set; }
        public DbSet<DexihDatalinkStep> DexihDatalinkStep { get; set; }
        public DbSet<DexihDatalinkStepParameter> DexihDatalinkStepParameters { get; set; }
	    public DbSet<DexihDatalinkStepColumn> DexihDatalinkStepColumns { get; set; }
        public DbSet<DexihDatalinkTarget> DexihDatalinkTargets { get; set; }
	    public DbSet<DexihDatalinkTest> DexihDatalinkTests { get; set; }
	    public DbSet<DexihDatalinkTestStep> DexihDatalinkTestSteps { get; set; }
	    public DbSet<DexihDatalinkTestTable> DexihDatalinkTestTables { get; set; }
        public DbSet<DexihDatalinkTransformItem> DexihDatalinkTransformItems { get; set; }
        public DbSet<DexihDatalinkTransform> DexihDatalinkTransforms { get; set; }
        public DbSet<DexihDatalink> DexihDatalinks { get; set; }
        public DbSet<DexihFileFormat> DexihFileFormats { get; set; }
        public DbSet<DexihFunctionParameter> DexihFunctionParameters { get; set; }
	    public DbSet<DexihFunctionArrayParameter> DexihFunctionArrayParameters { get; set; }
        public DbSet<DexihRemoteAgent> DexihRemoteAgents { get; set; }
	    public DbSet<DexihRemoteAgentHub> DexihRemoteAgentHubs { get; set; }
        public DbSet<DexihHubUser> DexihHubUser { get; set; }
        public DbSet<DexihHub> DexihHubs { get; set; }
        public DbSet<DexihHubVariable> DexihHubVariables { get; set; }
        public DbSet<DexihTableColumn> DexihTableColumns { get; set; }
        public DbSet<DexihTableIndex> DexihTableIndexes { get; set; }
        public DbSet<DexihTableIndexColumn> DexihTableIndexColumns { get; set; }
        public DbSet<DexihTable> DexihTables { get; set; }
        public DbSet<DexihTrigger> DexihTriggers { get; set; }
        public DbSet<DexihViewParameter> DexihViewParameters { get; set; }
	    public DbSet<DexihView> DexihViews { get; set; }
        public DbSet<DexihListOfValues> DexihListOfValues { get; set; }
        
        public DbSet<DexihIssue> DexihIssues { get; set; }
        public DbSet<DexihIssueComment> DexihIssueComments { get; set; }
        public DbSet<DexihTag> DexihTags { get; set; }
        public DbSet<DexihTagObject> DexihTagObjects { get; set; }
    }
}