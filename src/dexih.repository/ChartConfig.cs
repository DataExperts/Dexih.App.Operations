using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class ChartConfig
    {
      
        
        [Key(0)]
        public string LabelColumn { get; set; }

        [Key(1)]
        public string SeriesColumn { get; set; }

        [Key(2)]
        public string[] SeriesColumns { get; set; }

        [Key(3)]
        public string XColumn { get; set; }

        [Key(4)]
        public string YColumn { get; set; }

        [Key(5)]
        public string MinColumn { get; set; }

        [Key(6)]
        public string MaxColumn { get; set; }

        [Key(7)]
        public string RadiusColumn { get; set; }

        [Key(8)]
        public string LatitudeColumn { get; set; }

        [Key(9)]
        public string LongitudeColumn { get; set; }

        [Key(10)] 
        public EChartType ChartType { get; set; } = EChartType.BarVertical;

        [Key(11)] 
        public string ColorScheme { get; set; } = "natural";

        [Key(12)]
        public bool ShowGradient { get; set; }

        [Key(13)] public bool ShowXAxis { get; set; } = true;

        [Key(14)]
        public bool ShowYAxis { get; set; } = true;

        [Key(15)]
        public bool ShowLegend { get; set; }

        [Key(16)]
        public string LegendPosition { get; set; }

        [Key(17)]
        public bool ShowXAxisLabel { get; set; } = true;

        [Key(18)]
        public bool ShowYAxisLabel { get; set; } = true;

        [Key(19)]
        public bool ShowGridLines { get; set; }

        [Key(20)]
        public string XAxisLabel { get; set; }

        [Key(21)]
        public string YAxisLabel { get; set; }

        [Key(22)]
        public double? XScaleMax { get; set; }

        [Key(23)]
        public double? XScaleMin { get; set; }

        [Key(24)]
        public double? YScaleMax { get; set; }

        [Key(25)]
        public double? YScaleMin { get; set; }

        [Key(26)]
        public bool AutoScale { get; set; } = true;

        // pie charts only
        [Key(27)]
        public bool ExplodeSlices { get; set; }

        [Key(28)]
        public bool Doughnut { get; set; }
    }
}