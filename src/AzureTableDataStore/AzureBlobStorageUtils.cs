using System;
using System.Linq;
using Azure.Storage;

namespace AzureTableDataStore
{
    public class AzureBlobStorageUtils
    {
        // See https://github.com/Azure/azure-sdk-for-net/issues/12414
        public static StorageSharedKeyCredential GetCredentialFromConnectionString(string connectionString)
        {
            const string accountNameLabel = "AccountName";
            const string accountKeyLabel = "AccountKey";
            const string devStoreLabel = "UseDevelopmentStorage";

            const string devStoreAccountName = "devstoreaccount1";
            const string devStoreAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
            const string errorMessage = "The connection string must have an AccountName and AccountKey or UseDevelopmentStorage=true";

            try
            {
                var connectionStringValues = connectionString.Split(';')
                    .Select(s => s.Split(new char[] { '=' }, 2))
                    .ToDictionary(s => s[0], s => s[1]);

                string accountName;
                string accountKey;
                if (connectionStringValues.TryGetValue(devStoreLabel, out var devStoreValue) && devStoreValue == "true")
                {
                    accountName = devStoreAccountName;
                    accountKey = devStoreAccountKey;
                }
                else
                {
                    if (connectionStringValues.TryGetValue(accountNameLabel, out var accountNameValue)
                        && !string.IsNullOrWhiteSpace(accountNameValue)
                        && connectionStringValues.TryGetValue(accountKeyLabel, out var accountKeyValue)
                        && !string.IsNullOrWhiteSpace(accountKeyValue))
                    {
                        accountName = accountNameValue;
                        accountKey = accountKeyValue;
                    }
                    else
                    {
                        throw new ArgumentException(errorMessage);
                    }
                }

                return new StorageSharedKeyCredential(accountName, accountKey);
            }
            catch (IndexOutOfRangeException)
            {
                throw new ArgumentException(errorMessage);
            }
        }
	}
}