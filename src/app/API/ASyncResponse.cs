using automation.components.data.v1.CustomTypes;
using automation.components.operations.v1;
using automation.components.operations.v1.JSonExtensions;
using automation.components.operations.v1.JSonExtensions.Converters;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace automation.components.data.v1.API
{
    [Serializable()]
    public class ASyncResponse<T>
    {
        public Status Status { get; private set; }
        public DateTime StatusAt { get; private set; }
        public Request<T> Request { get; private set; }
        public T Entity { get; private set; }
        public DateTime ReceivedAt { get; private set; }
        public String ReceivedBy { get; private set; }
        public List<string> Logs { get; private set; }
        public bool EnableLog { get; private set; }

        public ASyncResponse(T Request, SerializableDictionary Headers)
        {
            Init(Request, Headers, true);
        }

        public ASyncResponse(T Request, SerializableDictionary Headers, bool EnableLog)
        {
            Init(Request, Headers, EnableLog);
        }

        private void Init(T Request, SerializableDictionary Headers, bool EnableLog)
        {
            this.EnableLog = EnableLog;
            this.ReceivedAt = DateTime.UtcNow;
            this.ReceivedBy = Environment.MachineName;
            this.Request = new Request<T>(Request, Headers);
            SetStatus(API.Status.QUEUED_PROCESSING);
        }

        public void SetStatus(Status Status)
        {
            if (EnableLog)
                Logs.Add(string.Format(@"{0} - Status change, From ""{1}"" To ""{2}""", DateTime.UtcNow.ToString(), this.Status, Status));
            this.Status = Status;
        }

        public void SetEntity(T Entity)
        {
            if (EnableLog)
                Logs.Add(string.Format(@"{0} - Entity change, From ""{1}"" To ""{2}""", DateTime.UtcNow.ToString(), JSon.Serialize<T>(this.Entity), JSon.Serialize<T>(Entity)));
            this.Entity = Entity;
        }

        public void Set(Status Status, T Entity)
        {
            SetStatus(Status);
            SetEntity(Entity);
        }
    }

    [Serializable()]
    public class Request<T>
    {
        public T Entity { get; private set; }
        public SerializableDictionary Headers { get; private set; }

        public Request(T Entity, SerializableDictionary Headers)
        {
            this.Entity = Entity;
            this.Headers = Headers;
        }
    }

    [JsonConverter(typeof(StringEnumConverter))]
    public enum Status
    {
        QUEUED_PROCESSING,
        PROCESSING,
    }
}
