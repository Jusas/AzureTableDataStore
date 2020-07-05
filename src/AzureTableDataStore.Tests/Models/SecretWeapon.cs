namespace AzureTableDataStore.Tests.Models
{
    public class SecretWeapon
    {

        public enum WeaponPortability
        {
            Static,
            Portable
        }

        public class WeaponProperties
        {
            public WeaponPortability Portability { get; set; }
            public double Weight { get; set; }
        }

        
        public string Manufacturer { get; set; }
        
        [TablePartitionKey]
        public string Type { get; set; }
        
        [TableRowKey]
        public string ModelId { get; set; }
        public string Name { get; set; }
        public WeaponProperties Properties { get; set; }
    }
}