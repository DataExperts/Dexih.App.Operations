using System;

namespace dexih.operations
{
    public class TestResult
    {
        public long TestStepKey { get; set; }
        public string Name { get; set; }

        public DateTime StartDate { get; set; }
        public DateTime EndDate { get; set; }

        public bool TestPassed { get; set; } = true;
        
        public long RowsMissingFromTarget { get; set; }
        public long RowsMismatching { get; set; }
        public long RowsMissingFromSource { get; set; }
    }
}