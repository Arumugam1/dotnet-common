﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using automation.components.data.v1.QueryCondition;
using automation.components.data.v1;
using automation.components.data.v1.Query;
using automation.components.operations.v1;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Builders = MongoDB.Driver.Builders;
using MongoDB.Driver.GridFS;

namespace automation.components.data.v1.MongoDB
{
    /// <summary>
    /// Helper structure used to specify file information.
    /// </summary>
    public class FileDocumentStore<T> where T : class
    {
        /// <summary>
        /// The file path you want as a string. This does not need to be the
        /// path where the file came from. We can use whatever string we
        /// understand when processing the file's binary.
        /// </summary>
        public string FilePath { get; set; }
        /// <summary>
        /// The file content as a byte array. This array gets streamed into MongoDb.
        /// </summary>
        public byte[] FileContent { get; set; }
        /// <summary>
        /// This is not used for Set operations. This is generated by MongoDb and
        /// returned in the Get operations.
        /// </summary>
        public string Md5 { get; set; }
        /// <summary>
        /// This is not used in Set operations. This is returned and generated by
        /// MongoDb. This is the number of chunks that MongoDb split the file into.
        /// </summary>
        public int ChunkSize { get; set; }
        /// <summary>
        /// This is used to specify other information as metadata along with the file.
        /// This will be serialized to json data and stored in the MongoDb metadata.
        /// </summary>
        public T MetaData { get; set; }
        /// <summary>
        /// The data and time the file was uploaded into MongoDb.
        /// This does not need to be set during Set operations.
        /// It will be generated during the Set operation to Now.
        /// </summary>
        public DateTime UploadDate { get; set; }
    }

    /// <summary>
    /// This client can only be used to store files using GridFs in MongoDb.
    /// </summary>
    public class FileClient<T> : IClient<FileDocumentStore<T>> where T : class
    {
        #region Constants ::

        private const string ConnectionString = "mongodb://localhost:27017";
        private const string CollectionName = "ScriptManager";
        private const string FindKey = "filename";
        private const string SimpleQueryKey = "SimpleQuery";
        private const string OrQueryKey = "OrQuery";
        private const string AndQueryKey = "AndQuery";

        #endregion

        #region Private Members ::

        private readonly string _connectionString;
        private readonly string _collectionName;
        private MongoServer _server;
        private MongoClient _client;
        private MongoDatabase _database;

        #endregion

        #region Constructors ::

        /// <summary>
        /// Public default constructor that defaults the connection information.
        /// This information is currently using constants defined above.
        /// This should be moved to a configuration if this constructor will be used.
        /// This currently used a local instance of MongoDb with a default collection name.
        /// </summary>
        public FileClient()
        {
            //Default connection to constants
            _connectionString = ConnectionString;
            _collectionName = CollectionName;
            InitConnection(_connectionString, _collectionName);
        }

        /// <summary>
        /// This overloaded constructor is used to be able to supply the actual
        /// connection and collection information for where to connect to MongoDb.
        /// </summary>
        /// <param name="connectionString">The MongoDb connection string.</param>
        /// <param name="collection">The MongoDb collection name.</param>
        public FileClient(string connectionString, string collection)
        {
            _connectionString = connectionString;
            _collectionName = collection;
            InitConnection(_connectionString, _collectionName);
        }

        #endregion

        #region Get ::

        /// <summary>
        /// This GetAll actually does not return all file data due to memory issues.
        /// This only returns the information and metadata for all objects in the
        /// collection. If we return all data it will be stored in memory
        /// and some of our files will be quite large.
        /// </summary>
        /// <returns>A DataSet with only the file information and metadata.</returns>
        public DataSet<FileDocumentStore<T>> GetAll()
        {
            var ds = new DataSet<FileDocumentStore<T>>();
            var files = _database.GridFS.FindAll();
            foreach (var file in files)
            {
                ProcessFileInfoTransfer(file, ds);
            }
            return ds;
        }

        /// <summary>
        /// Get only supports returning one file. It supports Or and And queries but only
        /// ever returns a single item in the dataset. This is so that the query cannot
        /// return too much data and flood the memory on the server.
        /// </summary>
        /// <param name="query">A Query specifying what file to pull</param>
        /// <returns>A single item in a dataset with all of the file data.</returns>
        public DataSet<FileDocumentStore<T>> Get(BaseQuery<FileDocumentStore<T>> query)
        {
            if (query == null)
                return null;
            var ds = new DataSet<FileDocumentStore<T>>();

            List<BaseQueryCondition> conditions;
            //SimpleQuery - Always based on file path name.
            if (query.GetType().FullName.Contains(SimpleQueryKey))
            {
                var file = _database.GridFS.FindOne(Builders.Query.EQ(FindKey, ((QueryIdentifier)((SimpleQuery<FileDocumentStore<T>>)query).Query).Identifier));
                ProcessFileTransfer(file, ds);
            }
            //OrQuery
            else if (query.GetType().FullName.Contains(OrQueryKey))
            {
                conditions = ((OrQuery<FileDocumentStore<T>>)query).OrConditions;
                IMongoQuery queryDoc = new QueryDocument();
                queryDoc = conditions.Select(condition => ((QueryValue)condition)).
                    Aggregate(queryDoc, (current, curValueQuery) => Builders.Query.Or(current, Builders.Query.EQ(curValueQuery.LookFor, curValueQuery.Value.ToString())));

                ProcessQueryData(queryDoc, ds);
            }
            //And Query
            else if (query.GetType().FullName.Contains(AndQueryKey))
            {
                conditions = ((OrQuery<FileDocumentStore<T>>)query).OrConditions;
                IMongoQuery queryDoc = new QueryDocument();
                queryDoc = conditions.Select(condition => ((QueryValue)condition)).
                    Aggregate(queryDoc, (current, curValueQuery) => Builders.Query.And(current, Builders.Query.EQ(curValueQuery.LookFor, curValueQuery.Value.ToString())));

                ProcessQueryData(queryDoc, ds);
            }

            return ds;
        }

        /// <summary>
        /// This function has been added and is not part of the IClient interface.
        /// You will need to create an actual class instance to be able to call it.
        /// This function returns the information including metadata without the 
        /// actual binary data for the file.
        /// </summary>
        /// <param name="id">The actual Id for the document store.</param>
        /// <returns>A single data item with the information and metadata only.</returns>
        public DataItem<FileDocumentStore<T>> GetInfoById(string id)
        {
            var ds = new DataSet<FileDocumentStore<T>>();
            var file = _database.GridFS.FindOneById(ObjectId.Parse(id));
            ProcessFileInfoTransfer(file, ds);
            return ds.Count == 1 ? ds[0] : null;
        }

        #endregion

        #region Set ::

        /// <summary>
        /// This method saves a single file into the MongoDb GridFS collection.
        /// </summary>
        /// <param name="data">The file data to save.</param>
        /// <returns>True for success and false for failures.</returns>
        public bool Set(DataItem<FileDocumentStore<T>> data)
        {
            bool success;
            try
            {
                using (var fs = new MemoryStream(data.Entity.FileContent))
                {
                    var options = GetMongoGridFsCreateOptions(data);
                    var writeInfo = _database.GridFS.Upload(fs, data.Entity.FilePath, options);
                    success = writeInfo.Name == data.Entity.FilePath;
                }
            }
            catch
            {
                success = false;
            }

            return success;
        }

        /// <summary>
        /// This method saves multiple files into the MondoDb GridFS collection.
        /// </summary>
        /// <param name="dataSet">The files in a dataset to save.</param>
        /// <returns>True for success and false for failures.</returns>
        public bool Set(DataSet<FileDocumentStore<T>> dataSet)
        {
            return dataSet.Aggregate(true, (current, dataItem) => current & Set(dataItem));
        }

        #endregion

        #region Delete ::

        /// <summary>
        /// This method deletes a single file from the MongoDb GridFS collection.
        /// </summary>
        /// <param name="data">The file item to delete.</param>
        /// <returns>True for success and false for failures.</returns>
        public bool Delete(DataItem<FileDocumentStore<T>> data)
        {
            try
            {
                var file = _database.GridFS.FindOne(Builders.Query.EQ(FindKey, data.Entity.FilePath));
                _database.GridFS.DeleteById(file.Id);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// This method deletes multiple files from the MongoDb GridFS collection.
        /// </summary>
        /// <param name="dataSet">A dataset containing the files to delete.</param>
        /// <returns>True for success and false for failures.</returns>
        public bool Delete(DataSet<FileDocumentStore<T>> dataSet)
        {
            return dataSet.Aggregate(true, (current, dataItem) => current & Delete(dataItem));
        }

        /// <summary>
        /// This method deletes files from the MongoDb GridFS collection based on a query.
        /// </summary>
        /// <param name="query">A query that shall select the file(s) to delete.</param>
        /// <returns>True for success and false for failures.</returns>
        public bool Delete(BaseQuery<FileDocumentStore<T>> query)
        {
            if (query == null)
                return false;
            var ds = Get(query);
            return Delete(ds);
        }

        /// <summary>
        /// This method will clear the entire collection of all files.
        /// This is meant to be internal only.
        /// </summary>
        /// <returns>True for success and false for failures.</returns>
        internal bool PurgeEntireCollection()
        {
            try
            {
                _database.GridFS.Database.Drop();
                _database = _server.GetDatabase(_collectionName);
                return true;
            }
            catch
            {
                return false;
            }
        }

        #endregion

        #region Dispose ::

        ///// <summary>
        ///// This method disposes of the MopngoDb types.
        ///// </summary>
        //public void Dispose()
        //{
        //    _database = null;
        //    _client = null;
        //    _server = null;
        //}

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _database = null;
                _client = null;
                _server = null;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        #region Private Methods ::

        private void InitConnection(string connectionString, string collection)
        {
            _client = new MongoClient(connectionString);
            _server = _client.GetServer();
            _database = _server.GetDatabase(collection);
        }

        private static void ProcessFileTransfer(MongoGridFSFileInfo file, ICollection<DataItem<FileDocumentStore<T>>> ds)
        {
            if (file == null) return;
            using (var stream = file.OpenRead())
            {
                var bytes = new byte[stream.Length];
                stream.Read(bytes, 0, (int)stream.Length);
                var newItem = new DataItem<FileDocumentStore<T>>
                    {
                        Identifier = file.Id.RawValue.ToString(),
                        EntryTimeStamp = file.UploadDate
                    };
                var fileDoc = new FileDocumentStore<T>
                    {
                        FilePath = file.Name,
                        FileContent = bytes,
                        Md5 = file.MD5,
                        MetaData = file.Metadata == null ? null : JSon.Deserialize<T>(file.Metadata.ToJson()),
                        ChunkSize = file.ChunkSize,
                        UploadDate = file.UploadDate
                    };

                newItem.Entity = fileDoc;
                ds.Add(newItem);
            }
        }

        private static void ProcessFileInfoTransfer(MongoGridFSFileInfo file, ICollection<DataItem<FileDocumentStore<T>>> ds)
        {
            if (file == null) return;

            var newItem = new DataItem<FileDocumentStore<T>>
                {
                    Identifier = file.Id.RawValue.ToString(),
                    EntryTimeStamp = file.UploadDate
                };
            var fileDoc = new FileDocumentStore<T>
                {
                    FilePath = file.Name,
                    Md5 = file.MD5,
                    MetaData = file.Metadata == null ? null : JSon.Deserialize<T>(file.Metadata.ToJson()),
                    ChunkSize = file.ChunkSize,
                    UploadDate = file.UploadDate
                };

            newItem.Entity = fileDoc;
            ds.Add(newItem);
        }

        private static MongoGridFSCreateOptions GetMongoGridFsCreateOptions(DataItem<FileDocumentStore<T>> data)
        {
            MongoGridFSCreateOptions options;
            if (data.Entity.MetaData != null)
            {
                var metaData = BsonSerializer.Deserialize<BsonDocument>(data.Entity.MetaData.ToJson());
                options = new MongoGridFSCreateOptions { Metadata = metaData, UploadDate = DateTime.Now, ChunkSize = data.Entity.FileContent.Length };
            }
            else
            {
                options = new MongoGridFSCreateOptions { UploadDate = DateTime.Now };
            }
            return options;
        }

        private void ProcessQueryData(IMongoQuery mongoQuery, ICollection<DataItem<FileDocumentStore<T>>> ds)
        {
            if (mongoQuery != null)
            {
                var file = _database.GridFS.FindOne(mongoQuery);
                ProcessFileTransfer(file, ds);
            }
        }

        #endregion
    }
}
