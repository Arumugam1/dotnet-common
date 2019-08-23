using System;
using System.Collections.Generic;
using System.DirectoryServices.Protocols;
using System.Reflection;

namespace automation.components.data.v1.DirectoryServices
{
    public class MyCustomMembershipUsers
    {
        private Dictionary<string, string> Mapping = new Dictionary<string, string>();

        public MyCustomMembershipUsers()
        { }

        public MyCustomMembershipUsers(SearchResultEntry User)
        {
            Mapping.Add("uid", "Username");
            Mapping.Add("givenName", "Firstname");
            Mapping.Add("sn", "Lastname");
            Mapping.Add("displayname", "Displayname");
            Mapping.Add("l", "City");
            Mapping.Add("co", "Country");
            Mapping.Add("telephoneNumber", "PhoneNumber");
            Mapping.Add("mail", "Email");

            Mapping.Add("directSupervisorEmail", "DirectSupervisorEmail");
            Mapping.Add("workShift", "WorkShift");
            Mapping.Add("homePhone", "HomePhone");
            Mapping.Add("employeeType", "EmployeeType");
            Mapping.Add("employeeStatus", "EmployeeStatus");
            Mapping.Add("title", "Title");
            Mapping.Add("cn", "CommonName");
            Mapping.Add("department", "ou");

            BindUser(User);
        }

        public String Username { get; set; }
        public String Firstname { get; set; }
        public String Lastname { get; set; }
        public String Displayname { get; set; }
        public String City { get; set; }
        public String Country { get; set; }
        public String PhoneNumber { get; set; }
        public String Email { get; set; }
        public String DirectSupervisorEmail { get; set; }
        public String WorkShift { get; set; }
        public String EmployeeType { get; set; }
        public String HomePhone { get; set; }
        public String EmployeeStatus { get; set; }
        public String Title { get; set; }
        public String CommonName { get; set; }
        public String ou { get; set; }
        public String Password { get; set; }
        public String[] Groups { get; set; }

        public void BindUser(SearchResultEntry User)
        {
            if (User == null)
            {
                return;
            }

            PropertyInfo CurProperty = default(PropertyInfo);
            foreach (String CurKey in Mapping.Keys)
            {
                CurProperty = this.GetType().GetProperty(Mapping[CurKey].ToString());
                if (CurProperty == null || User.Attributes[CurKey] == null || User.Attributes[CurKey].Count < 1 || null == User.Attributes[CurKey][0])
                    continue;

                CurProperty.SetValue(this, User.Attributes[CurKey][0].ToString(), null);
            }

            if (User.Attributes["groupmembership"] == null || User.Attributes["groupmembership"].Count < 1)
                return;

            //Note: groupmembership is not available in the new AD but it was in eDir. We do not need this information for now as we manage our own set of roles
            Groups = new String[User.Attributes["groupmembership"].Count];
            Int32 Count = 0;

            foreach (var CurGroup in User.Attributes["groupmembership"])
            {
                if (null != CurGroup)
                {
                    Groups[Count] = CurGroup.ToString();
                    Count++;
                }
            }
        }
    }
}
