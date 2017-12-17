using System;
using System.Collections.Generic;
using System.Text;

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
