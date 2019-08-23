using automation.components.operations.v1;
using System;
using System.Collections.Generic;
using System.Linq;

namespace automation.components.data.v1.Entities
{
    public class ElasticResponse<V>
    {
        public int took { get; set; }
        public bool time_out { get; set; }
        public ElasticResponseMainHit<V> hits { get; set; }
        public Dictionary<string, Dictionary<string, object>> aggregations { get; set; }

        public List<V> GetDocuments()
        {
            List<V> documentList = new List<V>();

            if (this.hits != null && this.hits.hits != null)
            {
                foreach (var item in this.hits.hits)
                {
                    documentList.Add(item._source);
                }
            }

            return documentList;
        }

        public List<string> GetAllIds()
        {
            List<string> idList = new List<string>();

            if (this.hits != null && this.hits.hits != null)
            {
                idList = this.hits.hits.Select(h => h._id).ToList();
            }

            return idList;
        }

        public int GetAggregationCount(string filterAggName) {

            if (this.aggregations != null && this.aggregations.ContainsKey(filterAggName)) {
                if (this.aggregations[filterAggName].ContainsKey("doc_count")) {
                    return Convert.ToInt32(this.aggregations[filterAggName]["doc_count"]);
                }
            }
            return 0;
        }

        public ElasticAggregationCount GetAggregationList(string filterAggName, string countAggName)
        {

            if (this.aggregations != null && this.aggregations.ContainsKey(filterAggName))
            {
                if (this.aggregations[filterAggName].ContainsKey(countAggName))
                {
                    return JSon.Deserialize<ElasticAggregationCount>(this.aggregations[filterAggName][countAggName].ToString());
                }
            }

            return default(ElasticAggregationCount);
        }
    }

    public class ElasticResponseMainHit<U>
    {
        public int total { get; set; }
        public float max_score { get; set; }
        public List<ElasticResponseHit<U>> hits { get; set; }
    }

    public class ElasticResponseHit<T>
    {
        public string _index { get; set; }
        public string _type { get; set; }
        public string _id { get; set; }
        public string _score { get; set; }
        public T _source { get; set; }
    }

    public class ElasticAggregationCount {
        public List<ElasticAggregationBuckets> buckets { get; set; }
    }
    public class ElasticAggregationBuckets {
        public string key { get; set; }
        public int doc_count { get; set; }
    }
}
