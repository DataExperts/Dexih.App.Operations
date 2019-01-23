using System;

namespace dexih.operations
{
    public class ColumnValidationException : Exception
    {
        public ColumnValidationException(string message): base(message)
        {

        }

        public ColumnValidationException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
