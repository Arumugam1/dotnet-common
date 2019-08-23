using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using automation.components.data.v1.Config;
using EtcdNet;

namespace automation.components.data.v1.Etcd
{
    public interface IDataExplorer<T>
    {
        bool Add(string key, string value);
        /// <summary>
        ///Add key with TTL
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <param name="timeToLive">TTL value in seconds</param>
        /// <returns></returns>
        bool Add(string key, string value, int timeToLive);
        bool Add(string directoryName,string key, string value);
        bool Add(string directoryName,string key, string value, int timeToLive);
        bool Add(string key, T value);
        bool Add(string key, T value, uint timeToLive);

        /// <summary>
        /// gets the data for the key
        /// </summary>
        /// <param name="key"></param>
        /// <returns>returns null if no key found</returns>
        EtcdResponse Get(string key);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns>returns null if no key found</returns>
        string GetValue(string key);
        List<T> Get(List<string> keys);
        string GetAsString(string key);
        Dictionary<string, string> GetAsString(List<string> keys);

        bool Delete(string key);

        bool AddIndex(string key, string indexKey, string indexValue);
        bool AddIndex(string key, string indexKey, string indexValue, uint timeToLive);


        string GetIndexValue(string key, string indexKey);

        bool DeleteIndex(string key);
        bool DeleteIndex(string key, string indexKey);
        bool DeleteIndex(string columnName, string key, string indexKey);

        int Count(string key);
        int Count(string key, int limit);


        bool ConnectivityCheck();
    }
    public abstract class DataExplorer<T> : IDataExplorer<T>, IDisposable
    {
        private const string KeyPrefix = "/";
        public bool Add(string key, string value)
        {
            return Add(key, value, 0);
        }

        public bool Add(string key, string value, int timeToLive)
        { 
            var client = GetEtcdClient();
            var result = timeToLive == 0
                ? Task.Run(() => client.SetNodeAsync(KeyPrefix + key, value)).Result
                : Task.Run(() => client.SetNodeAsync(KeyPrefix + key, value, timeToLive)).Result;
            return true;
        }

        public bool Add(string directoryName, string key, string value)
        {
           return Add(directoryName, key, value, 0);
        }

        public bool Add(string directoryName, string key, string value, int timeToLive)
        {
            var client = GetEtcdClient();
            var result = timeToLive == 0
                ? Task.Run(() => client.SetNodeAsync(KeyPrefix + directoryName + KeyPrefix + key, value)).Result
                : Task.Run(() => client.SetNodeAsync(KeyPrefix + directoryName + KeyPrefix + key, value, timeToLive))
                    .Result;
            return true;
        }

        public bool Add(string key, T value)
        {
            throw new NotImplementedException();
        }

        public bool Add(string key, T value, uint timeToLive)
        {
            throw new NotImplementedException();
        }

        public EtcdResponse Get(string key)
        {
            var client = GetEtcdClient();
            return Task.Run(() => client.GetNodeAsync(KeyPrefix + key, true)).Result;
        }

        public string GetValue(string key)
        {
            var client = GetEtcdClient();
            return Task.Run(() => client.GetNodeValueAsync(KeyPrefix + key, true)).Result;
        }


        public List<T> Get(List<string> keys)
        {
            throw new NotImplementedException();
        }

        public string GetAsString(string key)
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, string> GetAsString(List<string> keys)
        {
            throw new NotImplementedException();
        }

        public bool Delete(string key)
        {
            throw new NotImplementedException();
        }

        public bool AddIndex(string key, string indexKey, string indexValue)
        {
            throw new NotImplementedException();
        }

        public bool AddIndex(string key, string indexKey, string indexValue, uint timeToLive)
        {
            throw new NotImplementedException();
        }

        public string GetIndexValue(string key, string indexKey)
        {
            throw new NotImplementedException();
        }

        public bool DeleteIndex(string key)
        {
            throw new NotImplementedException();
        }

        public bool DeleteIndex(string key, string indexKey)
        {
            throw new NotImplementedException();
        }

        public bool DeleteIndex(string columnName, string key, string indexKey)
        {
            throw new NotImplementedException();
        }

        public int Count(string key)
        {
            throw new NotImplementedException();
        }

        public int Count(string key, int limit)
        {
            throw new NotImplementedException();
        }


        public bool ConnectivityCheck()
        {
            var client = GetEtcdClient();
            var result = Task.Run(() => client.SetNodeAsync(KeyPrefix + "ConnectivityCheck", "pass")).Result;
            var getKey = Task.Run(() => client.GetNodeValueAsync(KeyPrefix + "ConnectivityCheck")).Result;
            return getKey == "pass";
        }

        #region IDisposable Members::

        public virtual void Dispose()
        {

        }

        #endregion

        public virtual EtcdClient GetEtcdClient()
        {
            return EtcdConnection.Client;
        }
    }

    public static class EtcdConnection
    {
        static readonly Object ConnectionLock = new Object();
        private static EtcdClient _etcdClient;
        
        static EtcdConnection()
        {
        }
        public static EtcdClient Client
        {
            get
            {
                if (_etcdClient != null) return _etcdClient;
                lock (ConnectionLock)
                {
                    Connect();
                    
                }
                return _etcdClient;
            }
        }

        private static void Connect()
        {
            var urlList = Manager.GetApplicationConfigValue("URLLIST", "AllApplications.Etcd");
            if (string.IsNullOrEmpty(urlList))
            {
                throw new Exception("Etcd application config empty/missing");
            }

            var urlSplit = urlList.Split('#');
            ServicePointManager.SecurityProtocol = SecurityProtocolType.Ssl3 | SecurityProtocolType.Tls |
                                                   SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;

            EtcdClientOpitions options = new EtcdClientOpitions()
            {
                Urls = urlSplit,
                UseProxy = false,
                IgnoreCertificateError = true
            };

            _etcdClient = new EtcdClient(options);
        }
    }
}
