using System;

namespace dexih.operations
{
    public class TestResult
    {
        public long TestStepKey { get; set; }
        public string Name { get; set; }

        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }

        public bool RunSuccess { get; set; }
        public bool RowCountMatch { get; set; }
        public bool NaturalKeysMatch { get; set; }
        public bool TrackingColumnsMatch { get; set; }
    }
}