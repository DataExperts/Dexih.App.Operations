using System;
using System.Collections.Generic;
using System.Text;

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
