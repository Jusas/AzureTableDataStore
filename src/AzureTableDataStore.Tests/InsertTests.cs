using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using AzureTableDataStore.Tests.Models;
using Xunit;

namespace AzureTableDataStore.Tests
{
    public class InsertTests
    {

        [Fact]
        public async Task Should_insert_one_with_blobref()
        {

            var store = new TableDataStore<UserProfile>("UseDevelopmentStorage=true", "userprofiles",
                "userprofilesblobs", PublicAccessType.None, "UseDevelopmentStorage=true");

            string imageBase64 =
                "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAIAAAD8GO2jAAAACXBIWXMAAA7EAAAOxAGVKw4bAAAIo0lEQVRIiU2WSYwcZxXH3/JVVa/Te8+M7Rl7vEMWO4FYcQwhIQQUQQQIEOEQEiIQkEtA4ghXkFgkDohIcOXEJoSQglCIEmI7cWIntmM7Hnvs2ffpWbq7qruW7z0ONTPKdyiVSq++//u+t/3wRy+9yEiKzGSImYwhJkJGJCJGZEREBABQBQBFIgRURAVQgPSBAoCgqqCg6QukP4FJbZAQcNtaARQUQFUFABBIYVsDUx0EUAAEVE2/be8OAOk2H1kmiSMlQhBVI4KkasRYRMXtRUSElAoTIiIhggKmXiCAAu64i7u7C4juCggR20SJlQyLBTFIJvUQCZVYgBAADCuAJOkpFZWQUNQiEgEhIgDijpjdvgYwRKwqVgQQQa0IClpCgtRPVbVgXAcRNtu+qh0YyHuei0hhGCVJ4niutaKSqsru7SsAqCqgSWwCKogICsyprgoQECFgouI6PD07d+GdK72wd/zYaLGQaQw2Dxw65habH7x3PQ43D47tY3biOEojoYqqqqqiogp88v5jVmIRK2pFEhGb5oKqFbWO68zNLv3lb/9aXpjL5LhWLS4uLlYadck1Gns+pknh9q3ZuZnpjMeFQkasjZNERZnBc0yzkhsZLvP99x3dTQBAAkAFUAREVADD/N/X3lxZmmfXKRaz/SCo1Kul5lCh2gRXS+W9Q4OHC8XmzQ/vxElcLHrFQj7rcWezRdYn6w/VCnzvPYdVYSdbOI0VIACiYRME/bPn34njWNWqWM/jwT3NtY31pYkP/dYM5MqN4f3FUpV4QLQ0PTUdducX5yYdFoeT/527MD01axB3E8AQGmAk2i4x13HbW0EYhaDSaDZqlYLrmKSf+OurC5Nz9uCB5shY/t4HClw/cuAAk3vhwth//vl7R6ayZv/87FY/TNpBQMxMhETEbJiNMS4bl41L7LheZrPdkTgkNkHQD8Pw1EMPeg596syZhx57tLB3sHng6OBI2cd2R/xKzX3s82ee/Mr3LZTEhvl8Nozs5Ny6EREAJVREJCZEgu0mQUSmtb6mKqpaL2utnNvqdU+ceSRfHRz9zJcy+epmpz27PH/p6pvZTDVff5p62tgzuufQw2HvYpRg0ItczzE79ZoGNc0yARXDJgpDjJbrJdOo4ovfPvTBZH221elZKuaqVy5fN9l8qVZa2upvLM6/c/3V4T177tt/AjNRtrb30muvRr11Io59n0+eOL4TYUxTidIGxByF/dW5iW98Lv/i88dHRo689f7q5GKr24u8Yo3YnZq8vrIyszI3PXdnYvLi+wvz0/eefqjWqC6vdhTcO+NXoygOE+AHH/g4Ee80HNyuRAXHMa0N/8ToUqlCm3rm6W++cPvazas3Z7PVci/o50qVqNeeuHppY3Fhc2kJE+msrVZHhitDezpBf++BIzbu3rnxgeN4ZIwx7Bh2jXEdx3McBxFdzwt6UO+/94VHcO+++r76Rtx6/atPjRZRg8X5oNPyO6sLd251Vlf91Va41RUrcT9sr7eWV5am7l4t7S088bVn3EzGxqFJC4zZYWZCFCTHmF4QNPjaY0+2S7WhoX2SLWz2/P7BI+6zzxz71e/eXff7frc9f2s8CiNmFisgCqrsObN3xldnb3Y7x+bGb7muGwU9s9PmQQmByWFncX4h7F57/IvdxtHhTGYgjtElQxxuBWRowDXYXWvNbGyFfg8UhQRUkzAc3D+sGk/fuDy6r3bl9Vde+dOfi17GdZk/8cD9CIhACETMl6/cePPcpYc/WTt9eq/JDlkqiZbElHv9WqH0eLFQu3z5+kY78IN+FCUiggCGyGPef89hRMCgt7mwePWNs19+4oxYWVlbN2lbTSRkoOWFxfPnzoZhcvUa1fPJ6P581uuPHRqr1k5qlLt57cYvf/vyq+fHS5UBBYzjWAWMY8v5gea+hsbJ1vjtWj5/4ujhr58+WXYtRMH6Rhu/98K3EEFBiZhAp2fmV1Y3ml7QmV/urna++4Mnn3jmubfPXjx/4Y2//vv9duBmMiZKIkQM+3E60pqDlUatNLZvcKQxmPSDuBdMTEz6G61TZ05tgWesJIgKgCKJMe6RIwe/83Rh+dbtP1yb+ekvfvzZpx799W9e/sd/3ro51QFA11UVUaue5zaHy8eOHhaxfb/jMS9NLb579lLcSxwDhZyjVlaXW6c+/bCxNiYkQEBEKxIGXQv1t6/NP//Sc5VDR1/44c/eePtmpy+G2YpEURxFMQB4rlbLhbjnb7W7s3OL3U6PAApZKhY9w+QyWrR+pythYKIoYt5mE1ZNFP/493OeU7m73vn5sz9pbfq5XMaYOEnko6zQ9Xsf3pp1Xe4GISPkPeMyOpw2enUNs8N5D6OtNaOqIsJECqoixnGOHhnLZHIXL13x+2FloNCPI0BEhHRsbE8nACtCJuu4gioKkPWMx4AKiJBznayDUa83fWfWeF52h0XQGIfJQcB+GN+9O0NkwiRWVQJA5hisyrYGIopqu91BYs8hJGBEh9mK9QgzBjzPcTyn4/cMsgsASGyMMcZBBWPM3ML82nqLkEATgNR9NYyCKhZgB5nS01hRZErpiBEHct5AMaMKVgAIjEgMgJQCoQgCEMjM9EwUxq7rpLOZiJEUgUBFHRTVJJEdzlMiynoeIBTzuQzbgbwHiCAJIPqhNSAJEaHENrE2RSr1VlZWVUFF0uzaRg0VYjJEDCpWUqhTBSbwHPY0yeayx8fqcT/c3OqKRSLUMDJJFJNhMIAK1lpE6vf9za0tSu9FNBFU3GZaay2wMJPLlIglw9ZqYsXJ5gc8qJWzhYFK6IZRZG0SI4KIGt1Z6YwkIhEVSacbqoDgrgEwoYACACEgIhKDTSSRzY5fLzWDSMYnZvOFbK1WTeKoUi6HPd+k2Jmiq2HDzNaqiFUAqyqqoLCL71ZAVdXGnmFVFSsAgITtTre1kXW4TElUqw8IOwXPjcKoWqv/H5rE7CiHEWpYAAAAAElFTkSuQmCC";

            var imageStream = new MemoryStream(Convert.FromBase64String(imageBase64));
            var imageStream2 = new MemoryStream(Convert.FromBase64String(imageBase64));

            var userProfile = new UserProfile()
            {
                Name = "William",
                Age = 45,
                UserId = "222",
                UserType = "admin",
                Aliases = new List<string>() { "Bill", "Will" },
                ProfileImagery = new UserProfile.ProfileImages()
                {
                    Current = new LargeBlob("image.png", imageStream),
                    Old = new LargeBlob("older_me.png", imageStream2)
                }
            };

            await store.InsertAsync(false, userProfile);


        }

        [Fact]
        public async Task Should_insert_a_batch()
        {

            var testEntities = new List<SecretWeapon>();

            for (var i = 0; i < 105; i++)
            {
                testEntities.Add(new SecretWeapon()
                {
                    Manufacturer = "MI6 Skunk Works",
                    ModelId = Guid.NewGuid().ToString(),
                    Name = $"Walther PPK ({i})",
                    Type = "Personal Weapon",
                    Properties = new SecretWeapon.WeaponProperties()
                    {
                        Portability = SecretWeapon.WeaponPortability.Portable,
                        Weight = 1
                    }
                });
            }



            var store = new TableDataStore<SecretWeapon>("UseDevelopmentStorage=true", "secretweapons",
                null, PublicAccessType.None, null);

            await store.InsertAsync(true, testEntities.ToArray());




        }
    }
}
