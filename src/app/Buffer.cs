using System;
using System.Collections.Generic;

namespace automation.components.data.v1.Buffer
{
    public static class Manager<T>
    {
        public static T Get<T>(Dictionary<string, Entity<T>> bufferDB, string key)
        {
            if (!String.IsNullOrEmpty(key) && bufferDB.ContainsKey(key))
            {
                var bufferedEntity = bufferDB[key];

                if (bufferedEntity != default(Entity<T>) &&
                        bufferedEntity.Value != null &&
                        !bufferedEntity.Value.Equals(default(T)) &&
                        bufferedEntity.CacheUntil != default(DateTime) &&
                        bufferedEntity.CacheUntil > DateTime.UtcNow)
                    return bufferedEntity.Value;
            }

            return default(T);
        }

        public static void Set(Dictionary<string, Entity<T>> bufferDB, string key, T Entity, TimeSpan CacheUntil, object Lock)
        {
            if (!String.IsNullOrEmpty(key) &&
                Entity != null &&
                !Entity.Equals(default(T)) &&
                CacheUntil != default(TimeSpan) &&
                CacheUntil.TotalMilliseconds > 0)
                if (bufferDB.ContainsKey(key))
                    lock (Lock)
                        bufferDB[key] = new Entity<T> { Value = Entity, CacheUntil = DateTime.UtcNow.Add(CacheUntil) };
                else
                    lock (Lock)
                        if (!bufferDB.ContainsKey(key))
                            bufferDB.Add(key, new Entity<T> { Value = Entity, CacheUntil = DateTime.UtcNow.Add(CacheUntil) });
        }

        public static void Delete(Dictionary<string, Entity<T>> bufferDB, string key, object Lock)
        {
            if (!String.IsNullOrEmpty(key) && bufferDB.ContainsKey(key))
                lock (Lock)
                    if (!String.IsNullOrEmpty(key) && bufferDB.ContainsKey(key))
                        bufferDB.Remove(key);
        }
    }

    public class Entity<T>
    {
        /// <summary>
        /// Application Config
        /// </summary>
        public T Value { get; set; }

        /// <summary>
        /// Application Config is cache until this datetime is reached
        /// </summary>
        public DateTime CacheUntil { get; set; }
    }
}