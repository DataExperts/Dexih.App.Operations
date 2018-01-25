using System;

namespace dexih.repository
{
    public class ApplicationUserException : Exception
    {
        public ApplicationUserException(string message): base(message)
        {

        }
    }
}
