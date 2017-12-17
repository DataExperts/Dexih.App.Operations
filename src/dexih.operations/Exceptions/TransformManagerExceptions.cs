using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.operations
{
    public class TransformManagerException : Exception
    {
        public TransformManagerException(string message): base(message)
        {

        }

        public TransformManagerException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
