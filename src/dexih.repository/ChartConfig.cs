using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class ChartConfig
    {
        // [JsonConverter(typeof(StringEnumConverter))]
        public enum EChartType {
            BarVertical = 1,
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
            LinearGauge,
            Map
        }
        
        [ProtoMember(1)]
        public string LabelColumn { get; set; }

        [ProtoMember(2)]
        public string SeriesColumn { get; set; }

        [ProtoMember(3)]
        public string[] SeriesColumns { get; set; }

        [ProtoMember(4)]
        public string XColumn { get; set; }

        [ProtoMember(5)]
        public string YColumn { get; set; }

        [ProtoMember(6)]
        public string MinColumn { get; set; }

        [ProtoMember(7)]
        public string MaxColumn { get; set; }

        [ProtoMember(8)]
        public string RadiusColumn { get; set; }

        [ProtoMember(9)]
        public string LatitudeColumn { get; set; }

        [ProtoMember(10)]
        public string LongitudeColumn { get; set; }

        [ProtoMember(11)]
        public EChartType ChartType { get; set; }

        [ProtoMember(12)]
        public string ColorScheme { get; set; }

        [ProtoMember(13)]
        public bool ShowGradient { get; set; }

        [ProtoMember(14)]
        public bool ShowXAxis { get; set; }

        [ProtoMember(15)]
        public bool ShowYAxis { get; set; }

        [ProtoMember(16)]
        public bool ShowLegend { get; set; }

        [ProtoMember(17)]
        public string LegendPosition { get; set; }

        [ProtoMember(18)]
        public bool ShowXAxisLabel { get; set; }

        [ProtoMember(19)]
        public bool ShowYAxisLabel { get; set; }

        [ProtoMember(20)]
        public bool ShowGridLines { get; set; }

        [ProtoMember(21)]
        public string XAxisLabel { get; set; }

        [ProtoMember(22)]
        public string YAxisLabel { get; set; }

        [ProtoMember(23)]
        public double? XScaleMax { get; set; }

        [ProtoMember(24)]
        public double? XScaleMin { get; set; }

        [ProtoMember(25)]
        public double? YScaleMax { get; set; }

        [ProtoMember(26)]
        public double? YScaleMin { get; set; }

        [ProtoMember(27)]
        public bool AutoScale { get; set; }

        // pie charts only
        [ProtoMember(28)]
        public bool ExplodeSlices { get; set; }

        [ProtoMember(29)]
        public bool Doughnut { get; set; }
    }
}