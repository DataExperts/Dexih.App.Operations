using System;

namespace dexih.repository
{
    public class RepositoryException : Exception
    {
        public RepositoryException(string message) : base(message)
        {

        }

        public RepositoryException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
