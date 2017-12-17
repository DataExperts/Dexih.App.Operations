using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.operations
{
    public class RepositoryManagerException : Exception
    {
        public RepositoryManagerException(string message): base(message)
        {

        }

        public RepositoryManagerException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
