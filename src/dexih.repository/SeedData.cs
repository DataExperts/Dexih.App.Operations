using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Identity;

namespace dexih.repository
{
    public class SeedData
    {
        public async Task UpdateReferenceData(RoleManager<IdentityRole> roleManager, UserManager<ApplicationUser> userManager)
        {
            try
            {
                if (roleManager != null && userManager != null)
                {
                    var roles = new [] { "ADMINISTRATOR", "MANAGER", "USER", "VIEWER" };

                    foreach (var role in roles)
                    {
                        if (!await roleManager.RoleExistsAsync(role))
                        {
                            var identityRole = new IdentityRole(role);
                            var identityResult = await roleManager.CreateAsync(identityRole);

                            if (!identityResult.Succeeded)
                            {
                                //configurationLogger.LogCritical(
                                //    String.Format("!identityResult.Succeeded after roleManager.CreateAsync(identityRole) for identityRole with roleName { 0} ", role));
                                //foreach (var error in identityResult.Errors)
                                //{
                                //    configurationLogger.LogCritical(
                                //        String.Format(
                                //            "identityResult.Error.Description: {0}",
                                //            error.Description));
                                //    configurationLogger.LogCritical(
                                //        String.Format(
                                //            "identityResult.Error.Code: {0}",
                                //         error.Code));
                                //}
                            }
                        }
                    }

                    var user = new ApplicationUser
                    {
                        UserName = "admin",
                        Email = "admin@dataexpertsgroup.com",
                        EmailConfirmed = true,
						IsInvited = true,
						IsEnabled = true,
						IsRegistered = true,
						Terms = true,
						Subscription = false,
						FirstName = "Admin",
						LastName = "User",
                        HubQuota = 9999,
                        InviteQuota = 9999
                    };

                    var adminUser = await userManager.FindByEmailAsync("admin@dataexpertsgroup.com");
                    if (adminUser == null)
                    {
                        await userManager.CreateAsync(user, "dexIH-1");
                        //configurationLogger.LogDebug("admin@dataexpertsgroup.com user created.");
                        adminUser = await userManager.FindByEmailAsync("admin@dataexpertsgroup.com");
                    }

                    if (!await userManager.IsInRoleAsync(adminUser, "ADMINISTRATOR"))
                    {
                        await userManager.AddToRoleAsync(adminUser, "ADMINISTRATOR");
                        //configurationLogger.LogDebug("admin@dataexpertsgroup.com  added to administrator role");
                    }
                }

//                var internalHub = await repoDbContext.DexihHubs.FirstOrDefaultAsync(c => c.IsInternal);
//                if (internalHub == null)
//                {
//                    internalHub = new DexihHub()
//                    {
//                        HubKey = 0,
//                        IsInternal = true,
//                        IsValid = true,
//                        Name = "Internal Hub - not for use."
//                    };
//
//                    await AddOrUpdateAsync(repoDbContext, b => b.HubKey, new DexihHub[] { internalHub });
//                }
            }
            catch (Exception ex)
            {
                throw new RepositoryException("Failed to create/update the repository seed data.", ex);
            }
        }

//        private async Task AddOrUpdateAsync<TEntity>(
//            DexihRepositoryContext repoDbContext,
//            Func<TEntity, object> propertyToMatch, IEnumerable<TEntity> entities)
//            where TEntity : class
//        {
//            var entries = await repoDbContext.Set<TEntity>().ToListAsync();
//
//            foreach (var item in entities)
//            {
//                //var entry = await repoDbContext.Set<TEntity>().SingleOrDefaultAsync(g => propertyToMatch(g).Equals(propertyToMatch(item)));
//                var entry = entries.SingleOrDefault(g => propertyToMatch(g).Equals(propertyToMatch(item)));
//                if(entry == null)
//                {
//                    repoDbContext.Set<TEntity>().Add(item);
//                } else
//                {
//                    item.CopyProperties(entry, true);
//                }
//            }
//
//            await repoDbContext.SaveChangesAsync();
//        }


    }
}
