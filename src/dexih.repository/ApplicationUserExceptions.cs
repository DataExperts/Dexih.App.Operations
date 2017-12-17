using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.repository
{
    public class ApplicationUserException : Exception
    {
        public ApplicationUserException(string message): base(message)
        {

        }
    }
}
