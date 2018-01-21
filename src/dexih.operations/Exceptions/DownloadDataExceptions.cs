using System;

namespace dexih.operations
{
    public class DownloadDataException : Exception
    {
        public DownloadDataException(string message): base(message)
        {

        }

        public DownloadDataException(string message, Exception ex) : base(message, ex)
        {

        }
    }
}
