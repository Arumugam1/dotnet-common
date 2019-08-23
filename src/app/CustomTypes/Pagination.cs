using automation.core.components.operations.v1;
using automation.core.components.operations.v1.JSonExtensions;
using automation.core.components.operations.v1.JSonExtensions.Converters;
using System.Collections.Generic;
using System.Collections.Specialized;

namespace automation.core.components.data.v1.CustomTypes
{
    public class Pagination<T>
    {
        public Pagination()
        {
            this.From = 0;
            this.PageSize = 10;
            this.Marker = string.Empty;
            this.Direction = Direction.Forward;
            this.Sort = Sort.asc;
            this.SortField = string.Empty;
        }

        public int From { get; set; }
        public int PageSize { get; set; }
        public string Marker { get; set; }
        public Direction Direction { get; set; }
        public Sort Sort { get; set; }
        public string SortField { get; set; }
        public Result<T> ResultSet { get; set; }


        public void PopulatePageDetails(NameValueCollection queryStrings)
        {            
            //from index of the page
            if (!string.IsNullOrEmpty(queryStrings["from"]))
            {
                this.From = Parser.ToInt(queryStrings["from"], this.From);
            }

            //size of the page
            if (!string.IsNullOrEmpty(queryStrings["pagesize"]))
            {
                this.PageSize = Parser.ToInt(queryStrings["pagesize"], this.PageSize);
            }

            //marker of the page to start or end with
            if (!string.IsNullOrEmpty(queryStrings["marker"]))
            {
                this.Marker = queryStrings["marker"];
            }

            //direction of the page, based on the marker
            if (!string.IsNullOrEmpty(queryStrings["direction"]))
            {
                Direction pageDirection;
                if (System.Enum.TryParse(queryStrings["direction"], out pageDirection))
                {
                    this.Direction = pageDirection;
                }
            }

            //sort direction for the paged results
            if (!string.IsNullOrEmpty(queryStrings["sort"]))
            {
                Sort sort;
                if (System.Enum.TryParse(queryStrings["sort"], out sort))
                {
                    this.Sort = sort;
                }
            }

            //sort field to sort the page results
            if (!string.IsNullOrEmpty(queryStrings["sortfield"]))
            {
                this.SortField = queryStrings["sortfield"];
            }
        }

        public void PopulatePageDetails(dynamic queryObject)
        {
            //from index of the page
            if (queryObject["from"] != null && queryObject["from"].HasValue)
            {
                this.From = Parser.ToInt(queryObject["from"].Value, this.From);
            }

            //size of the page
            if (queryObject["pagesize"] != null && queryObject["pagesize"].HasValue)            
            {
                this.PageSize = Parser.ToInt(queryObject["pagesize"].Value, this.PageSize);
            }

            //marker of the page to start or end with
            if (queryObject["marker"] != null && queryObject["marker"].HasValue)            
            {
                this.Marker = queryObject["marker"].Value;
            }

            //direction of the page, based on the marker
            if (queryObject["direction"] != null && queryObject["direction"].HasValue)            
            {
                Direction pageDirection;
                if (System.Enum.TryParse(queryObject["direction"].Value, out pageDirection))
                {
                    this.Direction = pageDirection;
                }
            }

            //sort direction for the paged results
            if (queryObject["sort"] != null && queryObject["sort"].HasValue)            
            {
                Sort sort;
                if (System.Enum.TryParse(queryObject["sort"].Value, out sort))
                {
                    this.Sort = sort;
                }
            }

            //sort field to sort the page results
            if (queryObject["sortfield"] != null && queryObject["sortfield"].HasValue)            
            {
                this.SortField = queryObject["sortfield"].Value;
            }
        }
    }

    public class Result<T>
    {
        public long TotalRecords { get; set; }
        public string FirstMarker { get; set; }
        public string LastMarker { get; set; }
        public int FirstPageNumber { get; set; }
        public List<T> ResultSet { get; set; }
    }

    [JsonConverter(typeof (StringEnumConverter))]
    public enum Direction
    {
        Forward,
        Backward
    }

    [JsonConverter(typeof(StringEnumConverter))]
    public enum Sort
    {
        asc,
        desc
    }

}
