namespace AzureTableDataStore.Tests.Models
{

    public class DeepStructuredInner
    {
        public DeepStructuredInner2 Level1 { get; set; }
    }

    public class DeepStructuredInner2
    {
        public DeepStructuredInner3 Level2 { get; set; }
    }

    public class DeepStructuredInner3
    {
        public string FinalProperty { get; set; }
    }

    public class DeepStructuredObject
    {
        public DeepStructuredInner Level0 { get; set; }
    }
}