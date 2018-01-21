using System;

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
