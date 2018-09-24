using System;

namespace dexih.operations
{
    public class DatalinkTestRunException : Exception
    {
        public DatalinkTestRunException(string message): base(message)
        {

        }

        public DatalinkTestRunException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
