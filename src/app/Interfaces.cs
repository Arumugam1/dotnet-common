using System;
using System.Collections.Generic;

namespace automation.core.components.data.v1
{
    public interface IClient<T> : IDisposable
    {
        DataSet<T> GetAll();
        DataSet<T> Get(Query.BaseQuery<T> query);
        Boolean Set(DataItem<T> data);
        Boolean Set(DataSet<T> dataSet);
        Boolean Delete(DataItem<T> data);
        Boolean Delete(DataSet<T> dataSet);
        Boolean Delete(Query.BaseQuery<T> query);
    }

    #region Document Client Interfaces ::

    public struct SearchTerm
    {
        public string SearchKey { get; set; }
        public string SearchValue { get; set; }
    }

    /// <summary>
    /// An enum representing the types of document searches one would want to perform.
    /// </summary>
    public enum DocumentSearchConditions
    {
        /// <summary>
        /// Expresses containment. Returns documents treating each term separately.
        /// Returns a max of N where N = All documents in collection.
        /// </summary>
        Contains,
        /// <summary>
        /// Treats each term in list as an Or boolean condition for the same document.
        /// Returns a max of 1
        /// </summary>
        Or,
        /// <summary>
        /// Treats each term in list as an And boolean condition for the same document.
        /// Returns a max of 1
        /// </summary>
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
        /// <param name="searchTermKey">The search key. This is a json like string. ex. book.author.birthday</param>
        /// <param name="searchTermValue">The value to search. ex. 01/01/1940</param>
        /// <returns></returns>
        Document<T> GetDocument(SearchTerm searchTerm);
        /// <summary>
        /// Gets a list of documents based on search terms.
        /// </summary>
        /// <param name="searchTerms">A dictionary of strings containing the search terms.</param>
        /// <param name="condition">
        /// The condition represents how the list is treated.
        /// </param>
        /// <returns>A list of documents matching the criteria.</returns>
        IList<Document<T>> GetDocuments(IList<SearchTerm> searchTerms, DocumentSearchConditions condition);
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
}
