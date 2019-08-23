namespace automation.components.data.v1.AppContainer
{
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using automation.components.data.v1.Config;
    using automation.components.operations.v1;
    using Dse;

    /// <summary>
    /// Provides the entity list that is stored in applicationconfig.applicationconfig in our Cassandra
    /// DB.  This purposely does not use our typical application config provider / data explorers because those
    /// rely on the application container which has not yet been initialized at this point which creates
    /// a circular dependency.
    /// </summary>
    public class DbConfigEntitiesProvider : IEntitiesProvider
    {
        public static readonly string DB_QUERY = "select value from applicationconfig.applicationconfig where key = ?";
        private readonly ISession session;
        private readonly IStatement configQuery;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbConfigEntitiesProvider"/> class.
        ///
        /// This constructor is provided to support mocking of the session only and is not intended for
        /// use in a production application.
        /// </summary>
        /// <param name="session">DB session to use when querying config</param>
        public DbConfigEntitiesProvider(ISession session)
        {
            this.session = session;
            this.configQuery = this.PrepareQuery(session, DB_QUERY);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbConfigEntitiesProvider"/> class.
        ///
        /// Constructor that initializes the class with the real session to our cassandra DB.
        /// </summary>
        public DbConfigEntitiesProvider()
            : this(Cassandra.DSEConnection.Session)
        {
        }

        /// <summary>
        /// Provides the list of entities from the cassandra db.
        /// </summary>
        /// <returns>List of entities as defined in cassandra db</returns>
        public IEnumerable<Entity> GetEntities()
        {
            IEnumerable<Row> rows = this.session.Execute(configQuery).GetRows();

            return rows
                .Where(x => x != null && x.GetColumn("value") != null && !x.IsNull("value"))
                .Select(row => JSon.Deserialize<ApplicationConfig>(row.GetValue<string>("value")))
                .Select(appConfig => JSon.Deserialize<List<Entity>>(appConfig.Value))
                .FirstOrDefault() ?? new List<Entity>();
        }

        private BoundStatement PrepareQuery(ISession s, string query)
        {
            var preparedStmt = s.Prepare(query);
            string environment = ConfigurationManager.AppSettings["SystemType"];
            string key = string.Format("{0}.{1}.{2}", environment, "AllApplications", "Entities");
            return preparedStmt.Bind(key);
        }
    }
}
