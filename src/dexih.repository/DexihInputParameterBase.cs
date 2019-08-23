namespace dexih.repository
{
    /// <summary>
    /// Base class used for defining input parameters passed into datalink, view, api, jobs etc.
    /// </summary>
    public class InputParameterBase: DexihHubNamedEntity
    {
        public string Value { get; set; }
    }
}