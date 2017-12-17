using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.operations
{
    public class DatalinkRunException : Exception
    {
        public DatalinkRunException(string message): base(message)
        {

        }

        public DatalinkRunException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
