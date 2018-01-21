using System;

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
