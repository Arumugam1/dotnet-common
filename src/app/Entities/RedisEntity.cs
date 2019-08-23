namespace automation.components.data.v1.Entities
{
    /// <summary>
    /// Redis entity
    /// </summary>
    /// <typeparam name="T">Dynamic object</typeparam>
    public class RedisEntity<T>
    {
        /// <summary>
        /// Gets or sets
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// Gets or sets
        /// </summary>
        public T Value { get; set; }
    }
}
