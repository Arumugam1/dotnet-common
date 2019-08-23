using System;
using System.Collections.Generic;
using System.Linq;
using automation.components.operations.v1;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Builders = MongoDB.Driver.Builders;

namespace automation.components.data.v1.MongoDB
{
    #region Document Client Interfaces ::

    public interface IDocumentValue
    {
        String StringValue { get; set; }
        Boolean IsInt32();
        Boolean IsInt64();
        Boolean IsDouble();
        Boolean IsDateTime();
        Boolean IsBoolean();
        Boolean IsString();

        Int32 ToInt32();
        Int64 ToInt64();
        Double ToDouble();
        DateTime ToDateTime();
        Boolean ToBoolean();
    }

    public class DocumentValue<T> : IDocumentValue where T : struct
    {
        #region Private Members ::

        public String StringValue { get; set; }
        private Int16 _int16;
        private Int32 _int32;
        private Int64 _int64;
        private Double _double;
        private DateTime _dateTime;
        private Boolean _boolean;

        #endregion

        #region Public Members ::

        public T Value { get; set; }


        int IDocumentValue.ToInt32()
        {
            if (!IsInt32())
                throw new ArgumentException("Value is not a Int32.");
            return _int32;
        }

        long IDocumentValue.ToInt64()
        {
            if (!IsInt64())
                throw new ArgumentException("Value is not a Int64.");
            return _int64;
        }

        double IDocumentValue.ToDouble()
        {
            if (!IsDouble())
                throw new ArgumentException("Value is not a Double.");
            return _double;
        }

        DateTime IDocumentValue.ToDateTime()
        {
            if (!IsDateTime())
                throw new ArgumentException("Value is not a DateTime.");
            return _dateTime;
        }

        bool IDocumentValue.ToBoolean()
        {
            if (!IsBoolean())
                throw new ArgumentException("Value is not a Boolean.");
            return _boolean;
        }

        public bool IsInt16()
        {
            return Int16.TryParse(Value.ToString(), out _int16);
        }

        public bool IsInt32()
        {
            return Int32.TryParse(Value.ToString(), out _int32);
        }

        public bool IsInt64()
        {
            return Int64.TryParse(Value.ToString(), out _int64);
        }

        public bool IsDouble()
        {
            return Double.TryParse(Value.ToString(), out _double);
        }

        public bool IsDateTime()
        {
            return DateTime.TryParse(Value.ToString(), out _dateTime);
        }

        public bool IsBoolean()
        {
            return Boolean.TryParse(Value.ToString(), out _boolean);
        }

        public bool IsString()
        {
            return !String.IsNullOrEmpty(StringValue);
        }

        #endregion
    }

    public class DocumentQuery
    {
        #region Constructors ::

        private DocumentQuery()
        {

        }

        private DocumentQuery(string key, string value)
            : this()
        {
            SearchKey = key;
            //The char is not used. It is only used to initialize the value to set the string.
            SearchValue = new DocumentValue<char> { StringValue = value };
        }

        private DocumentQuery(string key, List<IDocumentValue> valueList, bool checkValueInList)
            : this()
        {
            SearchKey = key;
            SearchList = valueList;
            SearchCondition = checkValueInList ? DocumentValueSearchConditions.In : DocumentValueSearchConditions.NotIn;
        }

        private DocumentQuery(string key, IDocumentValue value, DocumentValueSearchConditions condition)
            : this()
        {
            SearchKey = key;
            SearchValue = value;
            SearchCondition = condition;
        }

        #endregion

        #region Factory Methods ::

        public static DocumentQuery CreateStringSearchTerm(string key, string value)
        {
            return new DocumentQuery(key, value);
        }

        public static DocumentQuery CreateSearchTerm(string key, IDocumentValue value, DocumentValueSearchConditions condition)
        {
            return new DocumentQuery(key, value, condition);
        }

        public static DocumentQuery CreateInOrNotInSearchTerm(string key, List<IDocumentValue> valueList, bool checkValueInList)
        {
            return new DocumentQuery(key, valueList, checkValueInList);
        }

        #endregion

        #region Properties ::

        internal DocumentValueSearchConditions SearchCondition { get; set; }
        internal string SearchKey { get; set; }
        internal IDocumentValue SearchValue { get; set; }
        internal List<IDocumentValue> SearchList { get; set; }

        #endregion
    }

    public enum DocumentValueSearchConditions
    {
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        In,
        NotIn
    }

    public enum DocumentSearchCombinationConditions
    {
        Or,
        And
    }

    /// <summary>
    /// This type is used to represent a NoSql Document.
    /// </summary>
    /// <typeparam name="T">The type of the content.</typeparam>
    public class Document<T> where T : class
    {
        /// <summary>
        /// The NoSql Document Id.
        /// </summary>
        public string Id { get; private set; }

        /// <summary>
        /// The document content.
        /// </summary>
        public T Content { get; set; }

        internal void SetId(string id)
        {
            Id = id;
        }
    }

    /// <summary>
    /// This interface is for clients to NoSql document stores.
    /// </summary>
    /// <typeparam name="T">The type of the document content.</typeparam>
    public interface IDocumentClient<T> : IDisposable where T : class
    {
        /// <summary>
        /// Get every document from the collection
        /// </summary>
        /// <returns>A collection of documents.</returns>
        IList<Document<T>> GetAllDocuments();
        /// <summary>
        /// Get a single document from the collection.
        /// </summary>
        /// <param name="id">The Id of the document in the collection.</param>
        /// <returns>A single document.</returns>
        Document<T> GetDocument(string id);
        /// <summary>
        /// Gets a single document based on the search terms.
        /// </summary>
        /// <param name="searchTerm">The search key and value. This is a json like string. ex. book.author.birthday.
        /// The value to search. ex. 01/01/1940</param>
        /// <returns></returns>
        Document<T> GetDocument(DocumentQuery searchTerm);
        /// <summary>
        /// Gets a list of documents based on search terms.
        /// </summary>
        /// <param name="searchTerms">A dictionary of strings containing the search terms.</param>
        /// <param name="condition">
        /// The condition represents how the list is treated.
        /// </param>
        /// <returns>A list of documents matching the criteria.</returns>
        IList<Document<T>> GetDocuments(IList<DocumentQuery> searchTerms, DocumentSearchCombinationConditions condition);
        /// <summary>
        /// Saves a single document.
        /// </summary>
        /// <param name="document">The document to save.</param>
        /// <returns>true for success and false for failure.</returns>
        Boolean SaveDocument(Document<T> document);
        /// <summary>
        /// Saves all of the documents in the document list.
        /// </summary>
        /// <param name="documentList">A list of documents to save.</param>
        /// <returns>true for success and false for failure.</returns>
        Boolean SaveDocuments(IList<Document<T>> documentList);
        /// <summary>
        /// Deletes a single document from the collecton.
        /// </summary>
        /// <param name="id">The Id of the document in the collection.</param>
        /// <returns>true for success and false for failure.</returns>
        Boolean DeleteDocument(string id);
        /// <summary>
        /// Deletes a single document from the collection.
        /// </summary>
        /// <param name="document">The actual document to delete.</param>
        /// <returns>true for success and false for failure.</returns>
        Boolean DeleteDocument(Document<T> document);
        /// <summary>
        /// Deletes multiple document based on the list of Ids.
        /// </summary>
        /// <param name="idList">a list of document Ids.</param>
        /// <returns>true for success and false for failure.</returns>
        Boolean DeleteDocuments(IList<string> idList);
    }

    #endregion

    public sealed class DocumentClient<T> : IDocumentClient<T> where T : class
    {
        #region Constants ::

        private const string ConnectionString = "mongodb://localhost:27017";
        private const string CollectionName = "DocumentStore";
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

        public DocumentClient()
        {
            //Default connection to constants
            _connectionString = ConnectionString;
            _collectionName = CollectionName;
            InitConnection(_connectionString, _collectionName);
        }

        public DocumentClient(string connectionString, string collection)
        {
            _connectionString = connectionString;
            _collectionName = collection;
            InitConnection(_connectionString, _collectionName);
        }

        #endregion

        #region Get Documents ::

        IList<Document<T>> IDocumentClient<T>.GetAllDocuments()
        {
            var documents = _database.GetCollection(_collectionName).FindAll();
            var docList = new List<Document<T>>();
            foreach (var document in documents)
            {
                var json =
                    document.ToJson()
                            .Replace(ReplaceObjIdHead + document.GetElement(IdKey).Value + ReplaceObjIdTail,
                                     string.Empty);
                var doc = new Document<T>
                {
                    Content = JSon.Deserialize<T>(json)
                };
                doc.SetId(document.GetElement(IdKey).Value.ToString());
                docList.Add(doc);
            }
            return docList;
        }

        Document<T> IDocumentClient<T>.GetDocument(string id)
        {
            var document = _database.GetCollection(_collectionName).FindOneById(ObjectId.Parse(id));
            return ProcessDocument(document);
        }

        Document<T> IDocumentClient<T>.GetDocument(DocumentQuery searchTerm)
        {
            var query = BuildTermQuery(searchTerm);
            var document = _database.GetCollection(_collectionName).FindOne(query);
            return ProcessDocument(document);
        }

        IList<Document<T>> IDocumentClient<T>.GetDocuments(IList<DocumentQuery> searchTerms, DocumentSearchCombinationConditions condition)
        {
            IMongoQuery queryDoc = new QueryDocument();
            switch (condition)
            {
                case DocumentSearchCombinationConditions.Or:
                    queryDoc = BuildOrQuery(searchTerms);
                    break;
                case DocumentSearchCombinationConditions.And:
                    queryDoc = BuildAndQuery(searchTerms);
                    break;
            }
            var documents = _database.GetCollection(_collectionName).Find(queryDoc);
            return documents.Select(ProcessDocument).ToList();
        }

        #endregion

        #region Save Documents ::

        bool IDocumentClient<T>.SaveDocument(Document<T> document)
        {
            return SaveDocument(document);
        }

        bool IDocumentClient<T>.SaveDocuments(IList<Document<T>> documentList)
        {
            return documentList.Aggregate(true, (current, document) => current & SaveDocument(document));
        }

        #endregion

        #region Delete Documents ::

        bool IDocumentClient<T>.DeleteDocument(string id)
        {
            return DeleteDocumentById(id);
        }

        bool IDocumentClient<T>.DeleteDocument(Document<T> document)
        {
            var doc = _database.GetCollection(_collectionName).FindOne(Builders.Query.Where(document.ToJson()));
            return DeleteDocumentById(doc.GetElement(IdKey).Value.ToString());
        }

        bool IDocumentClient<T>.DeleteDocuments(IList<string> idList)
        {
            return idList.Aggregate(true, (current, id) => current & DeleteDocumentById(id));
        }

        #endregion

        #region Dispose ::

        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (disposing)
            {
                _database = null;
                _client = null;
                _server = null;
            }
        }

        #endregion

        #region Private Methods ::

        private void InitConnection(string connectionString, string collection)
        {
            _client = new MongoClient(connectionString);
            _server = _client.GetServer();
            _database = _server.GetDatabase(collection);
        }

        private static Document<T> ProcessDocument(BsonDocument document)
        {
            var json = document.ToJson()
                               .Replace(ReplaceObjIdHead + document.GetElement(IdKey).Value + ReplaceObjIdTail,
                                        string.Empty);
            var doc = new Document<T>
            {
                Content = JSon.Deserialize<T>(json)
            };
            doc.SetId(document.GetElement(IdKey).Value.ToString());
            return doc;
        }

        private bool DeleteDocumentById(string id)
        {
            var doc = _database.GetCollection(_collectionName).FindOneById(ObjectId.Parse(id));
            IMongoQuery query = new QueryDocument(doc.Elements);
            return !string.IsNullOrEmpty(id) && _database.GetCollection(_collectionName).Remove(query, RemoveFlags.Single).Ok;
        }

        private bool SaveDocument(Document<T> document)
        {
            return string.IsNullOrEmpty(document.Id) ? SaveDocumentContentOnly(document) : SaveEntireDocument(document);
        }

        private bool SaveEntireDocument(Document<T> document)
        {
            var json = document.ToJson();
            var doc = BsonSerializer.Deserialize<BsonDocument>(json);
            return _database.GetCollection(_collectionName).Insert(doc).Ok;
        }

        private bool SaveDocumentContentOnly(Document<T> document)
        {
            var json = document.Content.ToJson();
            var doc = BsonSerializer.Deserialize<BsonDocument>(json);
            return _database.GetCollection(_collectionName).Insert(doc).Ok;
        }

        private static IMongoQuery BuildAndQuery(IEnumerable<DocumentQuery> searchTerms)
        {
            IMongoQuery queryDoc = new QueryDocument();
            return searchTerms.Aggregate(queryDoc, (current, term) => Builders.Query.And(current, BuildTermQuery(term)));
        }

        private static IMongoQuery BuildOrQuery(IEnumerable<DocumentQuery> searchTerms)
        {
            IMongoQuery queryDoc = new QueryDocument();
            return searchTerms.Aggregate(queryDoc, (current, term) => Builders.Query.Or(current, BuildTermQuery(term)));
        }

        private static IMongoQuery BuildTermQuery(DocumentQuery searchTerm)
        {
            IMongoQuery query = null;
            var bsonVal = GetBsonValue(searchTerm.SearchValue);

            switch (searchTerm.SearchCondition)
            {
                case DocumentValueSearchConditions.Equal:
                    query = Builders.Query.EQ(searchTerm.SearchKey, bsonVal);
                    break;
                case DocumentValueSearchConditions.GreaterThan:
                    query = Builders.Query.GT(searchTerm.SearchKey, bsonVal);
                    break;
                case DocumentValueSearchConditions.GreaterThanOrEqual:
                    query = Builders.Query.GTE(searchTerm.SearchKey, bsonVal);
                    break;
                case DocumentValueSearchConditions.LessThan:
                    query = Builders.Query.LT(searchTerm.SearchKey, bsonVal);
                    break;
                case DocumentValueSearchConditions.LessThanOrEqual:
                    query = Builders.Query.LTE(searchTerm.SearchKey, bsonVal);
                    break;
                case DocumentValueSearchConditions.NotEqual:
                    query = Builders.Query.NE(searchTerm.SearchKey, bsonVal);
                    break;
                case DocumentValueSearchConditions.In:
                    var bsonInValues = searchTerm.SearchList.Select(GetBsonValue).ToList();
                    query = Builders.Query.In(searchTerm.SearchKey, bsonInValues);
                    break;
                case DocumentValueSearchConditions.NotIn:
                    var bsonNotInValues = searchTerm.SearchList.Select(GetBsonValue).ToList();
                    query = Builders.Query.In(searchTerm.SearchKey, bsonNotInValues);
                    break;
            }
            return query;
        }

        private static BsonValue GetBsonValue(IDocumentValue docValue)
        {
            BsonValue bsonVal = null;
            if (docValue.IsDateTime())
                bsonVal = new BsonDateTime(docValue.ToDateTime());
            if (docValue.IsInt32())
                bsonVal = new BsonInt32(docValue.ToInt32());
            if (docValue.IsInt64())
                bsonVal = new BsonInt64(docValue.ToInt64());
            if (docValue.IsDouble())
                bsonVal = new BsonDouble(docValue.ToDouble());
            if (docValue.IsBoolean())
                bsonVal = new BsonInt32(docValue.ToInt32());
            if (docValue.IsString())
                bsonVal = new BsonString(docValue.StringValue);
            return bsonVal;
        }

        #endregion
    }
}