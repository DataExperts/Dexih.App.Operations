using System;
using System.Collections.Generic;
using System.Text;

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
