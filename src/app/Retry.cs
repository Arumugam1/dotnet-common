using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
namespace automation.components.data.v1
{

    public enum RetryStatus
    {
        Success = 0,
        Failed = 1
    }

    public enum RetryOn
    {
        ReadTimeout = 0,
        WriteTimeout = 1,
        UnavailableTimeout = 2
    }

    public class Retry
    {
        /// <summary>
        /// A simple synchronous retry mechinism
        /// </summary>
        /// <typeparam name="TResult">The result type</typeparam>
        /// <param name="action">The lambda to execute, no parameters allowed, will return the type</param>
        /// <param name="retryInterval">The sleep time between retries in miliseconds</param>
        /// <param name="retryCount">How many times to retry after initial attempt</param>
        /// <param name="exceptionToRetry">The exception type on which to retry</param>
        /// <param name="exceptionHandler">What to call when retries are exhausted or thrown exception is not of the catchable type</param>
        /// <param name="methodName">The name of the method being retried</param>
        /// <param name="paramObjects">The parameters passed into the method</param>
        /// <param name="status">The status of the action, if it failed or not</param>
        /// <returns>Returns the type</returns>
        public static TResult Execute<TResult>(
          Func<TResult> action,
          int retryInterval,
          int retryCount,
          Type exceptionToRetry,
          Action<Exception, string, object[]> exceptionHandler,
          string methodName,
          object[] paramObjects,
          out RetryStatus status)
        {
            while (true)
            {
                try
                {
                    status = RetryStatus.Success;
                    return action();
                }
                catch (Exception ex)
                {
                    if (ex.GetType() == exceptionToRetry && retryCount-- > 0)
                    {
                        Thread.Sleep(retryInterval);
                    }
                    else
                    {
                        exceptionHandler(ex, methodName, paramObjects);
                        status = RetryStatus.Failed;
                        return default(TResult);
                    }
                }
            }
        }

        /// <summary>
        /// A simple asynchronous retry mechinism
        /// </summary>
        /// <typeparam name="TResult">The result type</typeparam>
        /// <param name="action">The lambda to execute, no parameters allowed, will return a `Task` that returns `TResult`</param>
        /// <param name="retryInterval">The sleep time between retries in miliseconds</param>
        /// <param name="retryCount">How many times to retry after initial attempt</param>
        /// <param name="exceptionToRetry">The exception type on which to retry</param>
        /// <param name="exceptionHandler">What to call when retries are exhausted or thrown exception is not of the catchable type</param>
        /// <param name="methodName">The name of the method being retried</param>
        /// <param name="paramObjects">The parameters passed into the method</param>
        /// <returns>Returns an awaitable task that returns an instance of TResult</returns>
        public static Task<TResult> ExecuteAsync<TResult>(
          Func<Task<TResult>> action,
          int retryInterval,
          int retryCount,
          Type exceptionToRetry,
          Action<Exception, string, object[]> exceptionHandler,
          string methodName,
          object[] paramObjects)
        {
            return action().ContinueWith(task => RetryTask(task, action, retryInterval, retryCount, exceptionToRetry, exceptionHandler, methodName, paramObjects));
        }

        private static TResult RetryTask<TResult>(
          Task<TResult> task,
          Func<Task<TResult>> taskProvider,
          int retryInterval,
          int retryCount,
          Type exceptionToRetry,
          Action<Exception, string, object[]> exceptionHandler,
          string methodName,
          object[] paramObjects)
        {
            if (task.IsFaulted)
            {
                if (retryCount-- > 0 && task.Exception.InnerException.GetType() == exceptionToRetry)
                {
                    Thread.Sleep(retryInterval);
                    return taskProvider().ContinueWith(retry => RetryTask(retry, taskProvider, retryInterval, retryCount, exceptionToRetry, exceptionHandler, methodName, paramObjects)).Result;
                }
                else
                {
                    exceptionHandler(task.Exception.InnerException, methodName, paramObjects);
                    return default(TResult);
                }
            }

            return task.Result;
        }
    }
}
