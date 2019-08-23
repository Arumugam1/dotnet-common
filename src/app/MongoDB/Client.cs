using System;
using System.Collections.Generic;
using System.Linq;
using automation.components.data.v1.QueryCondition;
using automation.components.data.v1;
using automation.components.data.v1.Query;
using automation.components.operations.v1;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Builders = MongoDB.Driver.Builders;

namespace automation.components.data.v1.MongoDB
{
    public class Client<T> : IClient<T> where T : class
    {
        #region Constants ::

        private const string ConnectionString = "mongodb://localhost:27017";
        private const string CollectionName = "DocumentStore";
        private const string SimpleQueryKey = "SimpleQuery";
        private const string OrQueryKey = "OrQuery";
        private const string AndQueryKey = "AndQuery";
        private const string IdKey = "_id";
        private const string ReplaceObjIdHead = "\"_id\" : ObjectId(\"";
        private const string ReplaceObjIdTail = "\"),";

        #endregion

        #region Private Members ::

        private readonly string _connectionString;
        private readonly string _collectionName;
        private MongoServer _server;
        private MongoClient _client;
        private MongoDatabase _database;

        #endregion

        #region Constructors ::
        
        public Client()
        {
            //Default connection to constants
            _connectionString = ConnectionString;
            _collectionName = CollectionName;
            InitConnection(_connectionString, _collectionName);
        }

        public Client(string connectionString, string collection)
        {
            _connectionString = connectionString;
            _collectionName = collection;
            InitConnection(_connectionString, _collectionName);
        } 

        #endregion

        #region Get ::
        
        public DataSet<T> GetAll()
        {
            var ds = new DataSet<T>();
            var documents = _database.GetCollection(_collectionName).FindAll();

            ds.AddRange(from document in documents
                        let json = document.ToJson().Replace(ReplaceObjIdHead + document.GetElement(IdKey).Value + ReplaceObjIdTail, string.Empty)
                        select new DataItem<T>
                            {
                                Entity = JSon.Deserialize<T>(json),
                                EntryTimeStamp = DateTime.UtcNow,
                                Identifier = document.GetElement(IdKey).Value.ToString()
                            });
            return ds;
        }

        public DataSet<T> Get(BaseQuery<T> query)
        {
            if (query == null)
                return null;
            var ds = new DataSet<T>();

            List<BaseQueryCondition> conditions;
            //SimpleQuery - Always based on file path name.
            if (query.GetType().FullName.Contains(SimpleQueryKey))
            {
                // This function works but has been commented out as it has a potential for being
                //dangerous. The Where actually supports javascript which could be used for DOS and injection attacks. 
                //var mongoQuery = Builders.Query.Where(((SimpleQuery<T>)query).Identifier);
                //ProcessQueryData(mongoQuery, ds);
            }
            //OrQuery
            else if (query.GetType().FullName.Contains(OrQueryKey))
            {
                conditions = ((OrQuery<T>)query).OrConditions;
                IMongoQuery queryDoc = new QueryDocument();
                queryDoc = conditions.Select(condition => ((QueryValue) condition)).
                    Aggregate(queryDoc, (current, curValueQuery) => Builders.Query.Or(current, Builders.Query.EQ(curValueQuery.LookFor, curValueQuery.Value.ToString())));

                ProcessQueryData(queryDoc, ds);
            }
            //And Query
            else if (query.GetType().FullName.Contains(AndQueryKey))
            {
                conditions = ((AndQuery<T>)query).AndConditions;
                IMongoQuery queryDoc = new QueryDocument();
                queryDoc = conditions.Select(condition => ((QueryValue)condition)).
                    Aggregate(queryDoc, (current, curValueQuery) => Builders.Query.And(current, Builders.Query.EQ(curValueQuery.LookFor, curValueQuery.Value.ToString())));

                ProcessQueryData(queryDoc, ds);
            }

            return ds;
        } 

        #endregion

        #region Set ::
        
        public bool Set(DataItem<T> data)
        {
            var json = data.Entity.ToJson();
            var doc = BsonSerializer.Deserialize<BsonDocument>(json);
            return _database.GetCollection(_collectionName).Insert(doc).Ok;
        }

        public bool Set(DataSet<T> dataSet)
        {
            return dataSet.Aggregate(true, (current, dataItem) => current & Set(dataItem));
        }

        #endregion

        #region Delete ::

        public bool Delete(DataItem<T> data)
        {
            try
            {
                var doc = _database.GetCollection(_collectionName).FindOneById(ObjectId.Parse(data.Identifier));
                IMongoQuery query = new QueryDocument(doc.Elements);
                return !string.IsNullOrEmpty(data.Identifier) && _database.GetCollection(_collectionName).Remove(query, RemoveFlags.Single).Ok;
            }
            catch
            {
                return false;
            }
        }

        public bool Delete(DataSet<T> dataSet)
        {
            return dataSet.Aggregate(true, (current, dataItem) => current & Delete(dataItem));
        }

        public bool Delete(BaseQuery<T> query)
        {
            if (query == null)
                return false;
            var ds = Get(query);
            return Delete(ds);
        } 

        #endregion

        #region Dispose ::

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

        private void ProcessQueryData(IMongoQuery mongoQuery, ICollection<DataItem<T>> ds)
        {
            if (mongoQuery == null) return;
            var documents = _database.GetCollection(_collectionName).Find(mongoQuery);
            foreach (var document in documents)
            {
                var json = document.ToJson();
                json = json.Replace(ReplaceObjIdHead + document.GetElement(IdKey).Value + ReplaceObjIdTail, string.Empty);
                ds.Add(new DataItem<T> { Entity = JSon.Deserialize<T>(json), EntryTimeStamp = DateTime.UtcNow, Identifier = document.GetElement(IdKey).Value.ToString()});
            }
        }

        #endregion
    }
}
