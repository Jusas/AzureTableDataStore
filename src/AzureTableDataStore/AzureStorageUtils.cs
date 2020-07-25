using System;
using System.Linq;
using Azure.Storage;
using Microsoft.Azure.Cosmos.Table;

namespace AzureTableDataStore
{
    internal class AzureStorageUtils
    {

        internal enum CredentialType
        {
            BlobStorage,
            TableStorage
        }

        // See https://github.com/Azure/azure-sdk-for-net/issues/12414
        internal static (string accountName, string accountKey) GetAccountNameAndKeyFromConnectionString(string connectionString)
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

                return (accountName, accountKey);
            }
            catch (IndexOutOfRangeException)
            {
                throw new ArgumentException(errorMessage);
            }
        }

        internal static string GetStorageUriFromConnectionString(string connectionString, CredentialType credentialType)
        {
            try
            {
                var connectionStringValues = connectionString.Split(';')
                    .Select(s => s.Split(new char[] { '=' }, 2))
                    .ToDictionary(s => s[0], s => s[1]);

                if (connectionStringValues.TryGetValue("UseDevelopmentStorage", out var devStoreValue) && devStoreValue == "true")
                {
                    if(credentialType == CredentialType.TableStorage)
                        return "http://127.0.0.1:10002/devstoreaccount1";
                    else
                        return "http://127.0.0.1:10000/devstoreaccount1";
                }
                else
                {
                    if (connectionStringValues.TryGetValue("AccountName", out var accountNameValue)
                        && !string.IsNullOrWhiteSpace(accountNameValue)
                        && connectionStringValues.TryGetValue("EndpointSuffix", out var endpointSuffix)
                        && !string.IsNullOrWhiteSpace(endpointSuffix))
                    {
                        // make url
                        return "https://" + accountNameValue + "." + (credentialType == CredentialType.TableStorage ? "table." : "blob.") + endpointSuffix;
                    }
                    else
                    {
                        throw new ArgumentException("Expected to see AccountName and EndpointSuffix in connection string");
                    }
                }

            }
            catch (IndexOutOfRangeException)
            {
                throw;
            }
        }

        internal static StorageSharedKeyCredential GetStorageSharedKeyCredentialFromConnectionString(string connectionString)
        {
            var creds = GetAccountNameAndKeyFromConnectionString(connectionString);
            return new StorageSharedKeyCredential(creds.accountName, creds.accountKey);
        }

        internal static StorageCredentials GetStorageCredentialsFromConnectionString(string connectionString)
        {
            var creds = GetAccountNameAndKeyFromConnectionString(connectionString);
            return new StorageCredentials(creds.accountName, creds.accountKey);
        }

    }
}