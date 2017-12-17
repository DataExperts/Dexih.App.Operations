using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.operations
{
    public class DatajobRunException : Exception
    {
        public DatajobRunException(string message): base(message)
        {

        }

        public DatajobRunException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
