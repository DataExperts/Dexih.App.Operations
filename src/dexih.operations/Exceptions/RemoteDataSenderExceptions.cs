using System;

namespace dexih.operations
{
    public class RemoteDataSenderException : Exception
    {
        public RemoteDataSenderException(string message): base(message)
        {

        }

        public RemoteDataSenderException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
