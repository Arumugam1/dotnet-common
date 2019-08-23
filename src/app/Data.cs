using automation.core.components.operations.v1;
using System;
using System.Collections.Generic;

namespace automation.core.components.data.v1
{
    public class DataItem<T> : IDisposable, IEquatable<DataItem<T>>
    {
        public String Identifier { get; set; }
        public T Entity { get; set; }

        public DateTime EntryTimeStamp { get; set; }

        #region IDisposable Members

        //public void Dispose()
        //{
        //    //This is clearing the Identifier value while adding to dataset.
        //    //Because of this we cannot do delete with result dataset.
        //    //TODO: Need to address this issue.
        //    //Identifier = String.Empty;
        //}

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                //This is clearing the Identifier value while adding to dataset.
                //Because of this we cannot do delete with result dataset.
                //TODO: Need to address this issue.
                //Identifier = String.Empty;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        public bool Equals(DataItem<T> other)
        {
            return this.Identifier.Equals(other.Identifier, StringComparison.OrdinalIgnoreCase);
        }
    }

    public class DataSet<T> : List<DataItem<T>>, IDisposable
    {
        public DateTime QueryStartTimeStamp { get; internal set; }
        public DateTime QueryEndTimeStamp { get; internal set; }
        //public Client<T>.ClientType ClientType { get; internal set; }

        #region IDisposable Members

        //public void Dispose()
        //{
        //    QueryStartTimeStamp = DateTime.MinValue;
        //    QueryEndTimeStamp = DateTime.MinValue;
        //}

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                QueryStartTimeStamp = DateTime.MinValue;
                QueryEndTimeStamp = DateTime.MinValue;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion

        public DataSet<T> Intersect<T1>(DataSet<T> second)
        {
            if (second == null || second.Count == 0)
                return this;

            DataSet<T> TmpDS = new DataSet<T>();
            TmpDS.QueryStartTimeStamp = this.QueryStartTimeStamp;
            TmpDS.QueryEndTimeStamp = this.QueryEndTimeStamp;

            foreach (DataItem<T> DataItem in second)
                if (this.Contains(DataItem))
                    TmpDS.Add(DataItem);

            return TmpDS;
        }

        public DataSet<T> Union<T1>(DataSet<T> second)
        {
            if (second == null || second.Count == 0)
                return this;

            DataSet<T> TmpDS = new DataSet<T>();
            TmpDS.QueryStartTimeStamp = this.QueryStartTimeStamp;
            TmpDS.QueryEndTimeStamp = this.QueryEndTimeStamp;

            foreach (DataItem<T> DataItem in second)
                if (!this.Contains(DataItem))
                    TmpDS.Add(DataItem);

            return TmpDS;
        }
    }
}

namespace automation.core.components.data.v1.Query
{
    public abstract class BaseQuery<T>
    {
        public BaseQuery()
        {
            this.RowsReturned = uint.MaxValue;
        }

        // Added this property to override the Rows returned by Riak Search query.
        public uint RowsReturned { get; set; }
    }

    public class SimpleQuery<T> : BaseQuery<T>
    {
        public QueryCondition.BaseQueryCondition Query { get; internal set; }

        public SimpleQuery(Type EntityType, String Identifier)
        {
            this.Query = new QueryCondition.QueryIdentifier(Identifier);
        }

        public SimpleQuery(String Identifier)
        {
            this.Query = new QueryCondition.QueryIdentifier(Identifier);
        }

        public SimpleQuery(QueryCondition.BaseQueryCondition Query)
        {
            this.Query = Query;
        }
    }

    public class OrQuery<T> : BaseQuery<T>
    {
        public List<QueryCondition.BaseQueryCondition> OrConditions { get; internal set; }

        public OrQuery(Type EntityType, List<QueryCondition.BaseQueryCondition> OrConditions)
        {
            this.OrConditions = OrConditions;
        }

        public OrQuery(List<QueryCondition.BaseQueryCondition> OrConditions)
        {
            this.OrConditions = OrConditions;
        }
    }

    public class AndQuery<T> : BaseQuery<T>
    {
        public List<QueryCondition.BaseQueryCondition> AndConditions { get; internal set; }

        public AndQuery(Type EntityType, List<QueryCondition.BaseQueryCondition> AndConditions)
        {
            this.AndConditions = AndConditions;
        }

        public AndQuery(List<QueryCondition.BaseQueryCondition> AndConditions)
        {
            this.AndConditions = AndConditions;
        }
    }

    public class ContainsQuery<T> : BaseQuery<T>
    {
        public String Value { get; internal set; }

        public ContainsQuery(Type EntityType, String Value)
        {
            this.Value = Value;
        }

        public ContainsQuery(String Value)
        {
            this.Value = Value;
        }
    }

    public class MixedQuery<T> : BaseQuery<T>
    {
        public List<BaseQuery<T>> Value { get; internal set; }

        public MixedQuery(List<BaseQuery<T>> Value)
        {
            this.Value = Value;
        }
    }

    /// <summary>
    /// Represents the max query. Used to get the max value of the Identifier.
    /// </summary>
    public class MaxQuery<T> : BaseQuery<T>
    {
        public MaxQuery()
        {
        }
    }
}

namespace automation.core.components.data.v1.QueryCondition
{
    public abstract class BaseQueryCondition
    {
        public String LookFor { get; internal set; }
    }

    public class QueryIdentifier : BaseQueryCondition
    {
        public String Identifier { get; internal set; }

        public QueryIdentifier(String Identifier)
        {
            this.Identifier = Identifier;
        }
    }

    public class QueryValue : BaseQueryCondition
    {
        public Object Value { get; internal set; }

        public QueryValue(String LookFor, String Value)
        {
            this.LookFor = LookFor;
            this.Value = Value;
        }

        public QueryValue(String LookFor, Int32 Value)
        {
            this.LookFor = LookFor;
            this.Value = Value;
        }

        public QueryValue(String LookFor, bool Value)
        {
            this.LookFor = LookFor;
            this.Value = Value;
        }
    }

    public class QueryRange : BaseQueryCondition
    {
        public String From { get; internal set; }
        public String To { get; internal set; }
        public Int32 FromInt { get; internal set; }
        public Int32 ToInt { get; internal set; }
        public Boolean IsInt { get; internal set; }

        public DateTime FromDate { get; internal set; }
        public DateTime ToDate { get; internal set; }
        public Boolean IsDate { get; internal set; }

        public QueryRange(String lookFor, Int32 fromRange, Int32 toRange)
        {
            IsDate = false;
            LookFor = lookFor;
            From = fromRange.ToString();
            To = toRange.ToString();
            FromInt = fromRange;
            ToInt = toRange;
            IsInt = true;
        }

        public QueryRange(String lookFor, DateTime fromRange, DateTime toRange)
        {
            LookFor = lookFor;
            From = fromRange.ToString("o");
            To = toRange.ToString("o");
            IsDate = true;
            FromDate = fromRange;
            ToDate = toRange;
        }

        public QueryRange(String lookFor, string fromRange, string toRange)
        {
            LookFor = lookFor;
            From = fromRange;
            To = toRange;
        }
    }
}