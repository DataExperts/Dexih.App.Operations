using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.repository
{
    public class ChartConfig
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EChartType {
            BarVertical,
            BarHorizontal,
            BarVertical2D,
            BarHorizontal2D,
            BarVerticalStacked,
            BarHorizontalStacked,
            BarVerticalNormalized,
            BarHorizontalNormalized,
            Pie,
            PieAdvanced,
            PieGrid,
            Line,
            Area,
            Polar,
            AreaStacked,
            AreaNormalized,
            Scatter,
            Error,
            Bubble,
            ForceDirected,
            HeatMap,
            TreeMap,
            Cards,
            Gauge,
            Map
        }
        
        public EChartType ChartType { get; set; }
        public string ColorScheme { get; set; }
        public bool ShowGradient { get; set; }
        public bool ShowXAxis { get; set; }
        public bool ShowYAxis { get; set; }
        public bool ShowLegend { get; set; }
        public bool ShowXAxisLabel { get; set; }
        public bool ShowYAxisLabel { get; set; }
        public bool ShowGridLines { get; set; }
        public string XAxisLabel { get; set; }
        public string YAxisLabel { get; set; }
        public double? XScaleMax { get; set; }
        public double? XScaleMin { get; set; }
        public double? YScaleMax { get; set; }
        public double? YScaleMin { get; set; }
        
        public string LabelColumn { get; set; }
        public string SeriesColumn { get; set; }
        public string[] SeriesColumns { get; set; }
        public string XColumn { get; set; }
        public string YColumn { get; set; }
        public string MinColumn { get; set; }
        public string MaxColumn { get; set; }
        public string RadiusColumn { get; set; }
        public string LatitudeColumn { get; set; }
        public string LongitudeColumn { get; set; }

        // pie charts only
        public bool ExplodeSlices { get; set; }
        public bool Doughnut { get; set; }
    }
}