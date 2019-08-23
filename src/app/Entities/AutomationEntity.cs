using System.Xml.Serialization;
using automation.core.components.operations.v1.JSonExtensions;

namespace automation.core.components.data.v1.Entities
{
    public class AutomationEntity<T>
    {
        #region Public Members::

        public bool HasChanges()
        {
            var tmpLastSavedAutomationEvent = Original;
            Original = default(T);
            var returnValue = !Equals(tmpLastSavedAutomationEvent);
            Original = tmpLastSavedAutomationEvent;
            return returnValue;
        }

        public void BeforeSave()
        {
            Original = default(T);
        }

        public void AfterSave(T original)
        {
            Original = original;
        }

        [XmlIgnore]
        [JsonIgnore]
        public T Original { get; set; }

        #endregion
    }
}
