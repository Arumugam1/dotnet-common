using System;
using System.Diagnostics;
using System.Threading;
using Dse;

namespace automation.components.data.v1.Cassandra
{
    /// <summary>
    /// RBARetryPolicy
    /// Blurb
    /// </summary>
    public class DSERBARetryPolicy : IRetryPolicy
    {
        private readonly int maxRead;
        private readonly int maxWrite;
        private readonly int maxUnavailable;
        private readonly int backoffTime;

        public int MaxReadAttempts
        {
            get { return this.maxRead; }
        }

        public int MaxWriteAttempts
        {
            get { return this.maxWrite; }
        }

        public int MaxUnavailableAttempts
        {
            get { return this.maxUnavailable; }
        }

        public int BackoffTime
        {
            get { return this.backoffTime; }
        }

        public bool traceEnabled { get; set; }

        public DSERBARetryPolicy(int maximumReadAttempts, int maximumWriteAttempts, int maximumUnavailableAttempts, int backoff,
            bool graphEnabled = true, bool traceEnabled = true)
        {
            this.maxRead = maximumReadAttempts;
            this.maxWrite = maximumWriteAttempts;
            this.maxUnavailable = maximumUnavailableAttempts;
            this.backoffTime = backoff;

            this.traceEnabled = traceEnabled;

            this.TraceInit();
        }

        private RetryDecision Retry(IStatement query, int nbRetry, int max, ConsistencyLevel cl, RetryOn retryOn)
        {
            if (retryOn == RetryOn.ReadTimeout && nbRetry >= max)
            {
                if(nbRetry >= (max + 1))
                    return RetryDecision.Rethrow();
            }
            else if (nbRetry >= max)
                return RetryDecision.Rethrow();

            this.Backoff();

            if (retryOn == RetryOn.ReadTimeout && nbRetry >= max)
                return RetryWithConsistencyLevelOne(query);
            else
                return RetryDecision.Retry(cl);
        }
 
        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            this.TraceRead(cl, requiredResponses, receivedResponses, dataRetrieved, nbRetry);
            return this.Retry(query, nbRetry, this.maxRead, cl, RetryOn.ReadTimeout);
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            this.TraceUnavailable(cl, requiredReplica, aliveReplica, nbRetry);
            return this.Retry(query, nbRetry, this.maxUnavailable, cl, RetryOn.UnavailableTimeout);
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            this.TraceWrite(cl, writeType, requiredAcks, receivedAcks, nbRetry);
            return this.Retry(query, nbRetry, this.maxWrite, cl, RetryOn.WriteTimeout);
        }

        private void Backoff()
        {
            Thread.Sleep(this.backoffTime);
        }

        private void Dispatch(string message)
        {
            if (this.traceEnabled)
                Trace.WriteLine(message);
        }
        private void TraceInit()
        {
            if (this.traceEnabled)
                Trace.WriteLine(
                      String.Format("DSERBARetryPolicy initialized maximums read/write/conn: {0}/{1}/{2}, backoff: {3}", this.maxRead, this.maxWrite, this.maxUnavailable, this.BackoffTime
                          ));
        }

        private void TraceRead(ConsistencyLevel cl, int required, int recieved, bool data, int retries)
        {

            string message = string.Format(
                "READ TIMEOUT: CL = {0}, Required = {1}, Recieved = {2}, Data Present = {3}, Retries = {4}",
                    cl, required, recieved, data, retries);

            this.Dispatch(message);
        }

        private void TraceWrite(ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            string message = string.Format(
                "WRITE TIMEOUT: CL = {0}, Write Type = {1}, Acks = {2}/{3}, Retries = {4}",
                    cl, writeType, requiredAcks, receivedAcks, nbRetry);

            this.Dispatch(message);
        }

        private void TraceUnavailable(ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            string message = string.Format(
                "CONNECTION TIMEOUT: CL = {0}, Replica = {1}/{2}, Retries = {3}",
                    cl, requiredReplica, aliveReplica, nbRetry);

            this.Dispatch(message);
        }

        private void TraceChangeConsistencyLevel(ConsistencyLevel cl)
        {
            string message = string.Format("Consistency level change: Level = {0}", cl);
            this.Dispatch(message);
        }

        private RetryDecision RetryWithConsistencyLevelOne(IStatement query)
        {
            this.TraceChangeConsistencyLevel(ConsistencyLevel.One);
            return RetryDecision.Retry(ConsistencyLevel.One);
        }
    }
}