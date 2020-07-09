using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using AzureTableDataStore.Tests.Models;

namespace AzureTableDataStore.Tests.MockData
{
    public class TelescopeMockDataGenerator
    {
        /// <summary>
        /// A small "realistic" data set to work on.
        /// </summary>
        private static List<Func<TelescopePackageProduct>> _smallDataSet
            = new List<Func<TelescopePackageProduct>>()
            {
                () => new TelescopePackageProduct()
                {
                    CategoryId = "telescopes-full",
                    ProductId = "omegon-ac-70-700-az2",
                    Name = "Omegon Telescope AC 70/700 AZ-2",
                    Description =
                        "The Omegon AC 70/700 telescope is your first taste of the world of astronomy. Practical observing with it is so simple that it highly suitable for children and adults alike.\r\n\r\n\r\nThe instrument is simple to understand and is very quick to set up, without any tools being required. Simply set it up, insert eyepiece and observe!",
                    MainImage = new LargeBlob("omegon-ac-70700-az2.png",
                        () => new FileStream("Resources/omegon-ac-70-700-az2.png", FileMode.Open, FileAccess.Read)),
                    PackageDepthMm = 2200,
                    PackageHeightMm = 500,
                    PackageWidthMm = 500,
                    AddedToInventory = new DateTime(2017, 1, 2, 0, 0, 0, DateTimeKind.Utc),
                    InternalReferenceId = new Guid(1,2,3,4,4,4,4,4,4,4,4),
                    SoldItems = 10,
                    SearchNames = new List<string>()
                        {"Omegon Telescope AC 70/700 AZ-2", "Omegon AC 70/700", "Omegon AZ-2"},
                    Specifications = new ProductSpec()
                    {
                        ApplicationDescription = "General visual observation of sky and nature",
                        ForAstrophotography = false,
                        ForVisualObservation = true,
                        Mount = new MountSpec()
                        {
                            Type = "AZ-2",
                            GotoControl = false,
                            Mounting = MountingType.Azimuthal,
                            Tracking = false
                        },
                        Optics = new OpticsSpec()
                        {
                            Type = "Refractor",
                            ApertureMm = 70,
                            ApertureRatioF = 10,
                            FocalLengthMm = 700
                        },
                        Tripod = new TripodSpec()
                        {
                            HeightDescription = "66-120mm adjustable",
                            Material = "Aluminum",
                            WeightKg = 2
                        }
                    }
                },
                () => new TelescopePackageProduct()
                {
                    CategoryId = "telescopes-full",
                    ProductId = "meade-telescope-n-2001000-lx85-goto",
                    Name = "Meade Telescope N 200/1000 LX85 GoTo",
                    Description =
                        "LX85: Ideal for ambitious beginners and experienced observers who want to get into astrophotography.\r\n\r\nThe N 200/1000 telescope - The 200mm aperture allows the observer to see a wealth of detail when deep sky observing. With 78% more light available than a 150mm telescope even the delicate spiral arms of many galaxies and structures in nebulae may be observed. Even globular clusters, such as the famous M13 cluster in Hercules, can be clearly resolved right out to the edge.",
                    MainImage = new LargeBlob("meade-telescope-n-2001000-lx85-goto.png",
                        () => new FileStream("Resources/meade-telescope-n-2001000-lx85-goto.png", FileMode.Open, FileAccess.Read)),
                    PackageDepthMm = 2500,
                    PackageHeightMm = 800,
                    PackageWidthMm = 700,
                    AddedToInventory = new DateTime(2018, 1, 2, 0, 0, 0, DateTimeKind.Utc),
                    InternalReferenceId = new Guid(2,2,3,4,4,4,4,4,4,4,4),
                    SoldItems = 11,
                    SearchNames = new List<string>() {"Meade 200/1000 GoTo", "LX85"},
                    Specifications = new ProductSpec()
                    {
                        ApplicationDescription = "General visual observation and astrophotography",
                        ForAstrophotography = true,
                        ForVisualObservation = true,
                        Mount = new MountSpec()
                        {
                            Type = "LX85",
                            GotoControl = true,
                            Mounting = MountingType.GermanEquatorial,
                            Tracking = true
                        },
                        Optics = new OpticsSpec()
                        {
                            Type = "Reflector",
                            ApertureMm = 200,
                            ApertureRatioF = 5,
                            FocalLengthMm = 1000
                        },
                        Tripod = new TripodSpec()
                        {
                            HeightDescription = "1105mm adjustable",
                            Material = "Aluminum",
                            WeightKg = 5.6
                        }
                    }
                },
                () => new TelescopePackageProduct()
                {
                    CategoryId = "telescopes-full",
                    ProductId = "omegon-dob-n-102640",
                    Name = "Omegon Dobson telescope N 102/640 DOB",
                    Description =
                        "Omegon 102/640 Dobsonian telescope -\r\n\r\na space-saving telescope for those who want to observe more than just the Moon.",
                    MainImage = null,
                    PackageDepthMm = 1200,
                    PackageHeightMm = 500,
                    PackageWidthMm = 700,
                    AddedToInventory = new DateTime(2019, 1, 2, 0, 0, 0, DateTimeKind.Utc),
                    InternalReferenceId = new Guid(3,2,3,4,4,4,4,4,4,4,4),
                    SoldItems = 12,
                    SearchNames = new List<string>() {"Omegon Dobson"},
                    Specifications = new ProductSpec()
                    {
                        ApplicationDescription = "Beginner visual observation",
                        ForAstrophotography = false,
                        ForVisualObservation = true,
                        Mount = new MountSpec()
                        {
                            Type = "Dobson",
                            GotoControl = false,
                            Mounting = MountingType.Dobsonian,
                            Tracking = false
                        },
                        Optics = new OpticsSpec()
                        {
                            Type = "Reflector",
                            ApertureMm = 102,
                            ApertureRatioF = 6.27,
                            FocalLengthMm = 640
                        },
                        Tripod = new TripodSpec()
                        {
                            HeightDescription = "not applicable",
                            Material = "Wood",
                            WeightKg = 5.6
                        }
                    }
                },
                () => new TelescopePackageProduct()
                {
                    CategoryId = "telescopes-ota",
                    ProductId = "c-sct-sc-2032032-c8-ota",
                    Name = "Celestron Schmidt-Cassegrain telescope SC 203/2032 C8 (OTA only)",
                    Description =
                        "Despite the long focal length of these telescopes, the OTA itself has a very short length, making the system a compact telescope which is extremely easy to transport.",
                    MainImage = null,
                    PackageDepthMm = 2500,
                    PackageHeightMm = 600,
                    PackageWidthMm = 600,
                    AddedToInventory = new DateTime(2020, 1, 2, 0, 0, 0, DateTimeKind.Utc),
                    InternalReferenceId = new Guid(4,2,3,4,4,4,4,4,4,4,4),
                    SoldItems = 13,
                    SearchNames = new List<string>() {"Celestron C8", "C8"},
                    Specifications = new ProductSpec()
                    {
                        ApplicationDescription = "Visual observing and astrophotography",
                        ForAstrophotography = true,
                        ForVisualObservation = true,
                        Mount = null,
                        Optics = new OpticsSpec()
                        {
                            Type = "Reflector",
                            ApertureMm = 203,
                            ApertureRatioF = 10,
                            FocalLengthMm = 2032
                        },
                        Tripod = null
                    }
                }
            };

        public static List<TelescopePackageProduct> SmallDataSet => _smallDataSet.Select(x => x()).ToList();


        /// <summary>
        /// Create a data set of the given size based on the small data set.
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public static TelescopePackageProduct[] CreateDataSet(int size, string additionalIdStr = "", string partitionKey = null)
        {
            var items = new TelescopePackageProduct[size];
            int i = 0;
            while (i < size)
            {
                var index = i % _smallDataSet.Count;
                var newItem = _smallDataSet[index]();
                newItem.ProductId += $"{i}"+additionalIdStr;
                newItem.Name += $" ({i})";
                if (partitionKey != null)
                    newItem.CategoryId = partitionKey;
                items[i] = newItem;
                i++;
            }

            return items;
        }
    }
}