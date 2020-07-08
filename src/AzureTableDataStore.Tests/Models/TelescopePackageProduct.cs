using System;
using System.Collections.Generic;

namespace AzureTableDataStore.Tests.Models
{
    public class TelescopePackageProduct
    {
        [TableRowKey]
        public string ProductId { get; set; }
        
        [TablePartitionKey]
        public string CategoryId { get; set; }


        public string Name { get; set; }
        public List<string> SearchNames { get; set; }
        public DateTime AddedToInventory { get; set; }
        public Guid InternalReferenceId { get; set; }
        public long SoldItems { get; set; }

        public int PackageWidthMm { get; set; }
        public int PackageHeightMm { get; set; }
        public int PackageDepthMm { get; set; }

        public string Description { get; set; }
        public LargeBlob MainImage { get; set; }

        public ProductSpec Specifications { get; set; }


    }

    public class ProductSpec
    {
        public string ApplicationDescription { get; set; }
        public bool ForAstrophotography { get; set; }
        public bool ForVisualObservation { get; set; }

        public OpticsSpec Optics { get; set; }
        public MountSpec Mount { get; set; }
        public TripodSpec Tripod { get; set; }
    }


    public class OpticsSpec
    {
        public string Type { get; set; }
        public int ApertureMm { get; set; }
        public int FocalLengthMm { get; set; }
        public double ApertureRatioF { get; set; }
    }

    public enum MountingType
    {
        Azimuthal,
        GermanEquatorial,
        AltAz,
        Dobsonian
    }

    public class MountSpec
    {
        public string Type { get; set; }
        public MountingType Mounting { get; set; }
        public bool GotoControl { get; set; }
        public bool Tracking { get; set; }
    }

    public class TripodSpec
    {
        public string HeightDescription { get; set; }
        public string Material { get; set; }
        public double WeightKg { get; set; }
    }


}