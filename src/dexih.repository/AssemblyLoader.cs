using System.Reflection;
#if NET462
#else
using System.Runtime.Loader;
#endif

namespace dexih.repository
{
#if NET462
    public class AssemblyLoader
    {

        public Assembly LoadFromAssemblyName(AssemblyName assemblyName)
        {
            var apiApplicationFileInfo = new FileInfo($"{folderPath}{Path.DirectorySeparatorChar}{assemblyName.Name}.dll");
            if (File.Exists(apiApplicationFileInfo.FullName))
            {
                return Assembly.LoadFrom(apiApplicationFileInfo.FullName);
            }
            else
            {
                return null;
            }
        }

        public Assembly LoadFromStream(MemoryStream stream)
        {
            return Assembly.Load(stream.ToArray());
        }
    }
#else
    public class AssemblyLoader : AssemblyLoadContext
    {
        
        protected override Assembly Load(AssemblyName assemblyName)
        {
            //var deps = DependencyContext.Default;
            //var res = deps.CompileLibraries.Where(d => d.Name.Contains(assemblyName.Name)).ToList();
            //if (res.Count > 0)
            //{
            //    return Assembly.Load(new AssemblyName(res.First().Name));
            //}
            //else
            //{
            //    var apiApplicationFileInfo = new FileInfo($"{folderPath}{Path.DirectorySeparatorChar}{assemblyName.Name}.dll");
            //    if (File.Exists(apiApplicationFileInfo.FullName))
            //    {
            //        var asl = new AssemblyLoader(apiApplicationFileInfo.DirectoryName);
            //        return asl.LoadFromAssemblyPath(apiApplicationFileInfo.FullName);
            //    }
            //}
            return Assembly.Load(assemblyName);
        }

    }
#endif
}
