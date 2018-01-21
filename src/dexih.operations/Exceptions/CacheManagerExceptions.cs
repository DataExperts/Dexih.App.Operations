using System;

namespace dexih.operations
{
    public class CacheManagerException : Exception
    {
        public CacheManagerException(string message): base(message)
        {

        }

        public CacheManagerException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
