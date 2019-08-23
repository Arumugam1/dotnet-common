using System;
using System.Collections.Generic;
using System.Linq;
using System.Xml.Serialization;
using automation.components.operations.v1;

namespace automation.components.data.v1.CustomTypes
{
    [Serializable()]
    [XmlRoot("dictionary")]
    public class SerializableDictionary : Dictionary<string, object>, IXmlSerializable
    {
        /// <summary>
        /// Serializes the object's value (if json or xml) or returns the string representation if the object is a value type.
        /// </summary>
        /// <param name="key">The collection item identifier.</param>
        /// <returns>String value representing the value of the object.</returns>
        public string GetFlattenedValue(string key)
        {
            try
            {
                return JSon.ValidateJson(this[key].ToString()) ? JSon.Serialize(this[key])
                           : XML.ValidateXML(this[key].ToString()) ? XML.Serialize(this[key].ToString())
                                 : this[key].ToString();
            }
            catch (Exception)
            {
                return string.Empty;
            }
        }

        public System.Xml.Schema.XmlSchema GetSchema()
        {
            return null;
        }

        public void ReadXml(System.Xml.XmlReader reader)
        {
            var rootNode = reader.LocalName;
            reader.ReadStartElement();

            do
            {
                var key = reader.LocalName;
                var value = reader.ReadElementContentAsObject();

                Add(key, value);

            } while (!reader.LocalName.Equals(rootNode, StringComparison.OrdinalIgnoreCase));

            reader.ReadEndElement();
        }

        public void WriteXml(System.Xml.XmlWriter writer)
        {
            foreach (var key in Keys.Where(key => this[key] != null))
            {
                writer.WriteStartElement("element");
                writer.WriteAttributeString("name", key);
                writer.WriteString(this[key].ToString());
                writer.WriteEndElement();
            }
        }
    }

    [Serializable()]
    public class SerializableDictionary<TKey, TValue> : Dictionary<TKey, TValue>, IXmlSerializable
    {
        private const string keyAttribute = "name";

        public System.Xml.Schema.XmlSchema GetSchema()
        {
            return null;
        }

        /// <summary>
        /// Generates an object from its XML representation.
        /// </summary>
        /// <param name="reader">The <see cref="T:System.Xml.XmlReader" /> stream from which the object is deserialized.</param>
        public void ReadXml(System.Xml.XmlReader reader)
        {
            var rootNode = reader.LocalName;
            reader.ReadStartElement();

            do
            {
                TKey key = default(TKey);
                TValue value = default(TValue);

                if (typeof(TKey).IsEnum)
                {
                    key = (TKey)Enum.Parse(typeof(TKey), reader.GetAttribute(keyAttribute));
                }
                else
                {
                    key = (TKey)Convert.ChangeType(reader.GetAttribute(keyAttribute), typeof(TKey));
                }

                value = XML.Deserialize<TValue>(reader.ReadInnerXml());

                Add(key, value);
            } while (!reader.LocalName.Equals(rootNode, StringComparison.OrdinalIgnoreCase));

            reader.ReadEndElement();
        }

        /// <summary>
        /// Converts an object into its XML representation.
        /// </summary>
        /// <param name="writer">The <see cref="T:System.Xml.XmlWriter" /> stream to which the object is serialized.</param>
        public void WriteXml(System.Xml.XmlWriter writer)
        {
            foreach (var key in Keys.Where(key => this[key] != null))
            {
                writer.WriteStartElement(typeof(TKey).Name);
                writer.WriteAttributeString(keyAttribute, key.ToString());
                writer.WriteRaw(XML.Serialize(this[key]));
                writer.WriteEndElement();
            }
        }
    }

    public static class Extensions
    {
        public static SerializableDictionary<TKey, TElement> ToSerializableDictionary<TSource, TKey, TElement>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector, Func<TSource, TElement> elementSelector)
        {
            var result = new SerializableDictionary<TKey, TElement>();

            foreach (TSource item in source)
            {
                result.Add(keySelector(item), elementSelector(item));
            }

            return result;
        }
    }
}
