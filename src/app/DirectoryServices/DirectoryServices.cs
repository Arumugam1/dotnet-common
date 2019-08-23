using System;
using System.DirectoryServices.Protocols;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using automation.components.data.v1.Config;

namespace automation.components.data.v1.DirectoryServices
{

    public class DirectoryServices
    {
        private const string DirSvcNameSpace = "DirectoryServices";

        private bool UseAD
        {
            get
            {
                return string.Equals(Manager.GetApplicationConfigValue("ARIC-9495-UseAD", "Aric.FeatureFlag"), "true", StringComparison.InvariantCultureIgnoreCase);
            }
        }

        private string DistinguishedNameForSearch
        {
            get
            {
                if (this.UseAD)
                {
                    if (null != Manager.GetApplicationConfigValue("DistinguishedNameForSearchForAD", DirSvcNameSpace))
                        return Manager.GetApplicationConfigValue("DistinguishedNameForSearchForAD", DirSvcNameSpace).ToString().Trim();
                    else
                        return string.Empty;
                }
                else
                    return "o=rackspace";
            }
        }

        private string DirSvcServer
        {
            get
            {
                var configValue = this.UseAD ? Manager.GetApplicationConfigValue("ADServer", DirSvcNameSpace) : Manager.GetApplicationConfigValue("eDirServer", DirSvcNameSpace);

                if (null != configValue)
                    return configValue.ToString().Trim();
                else
                    throw new Exception("Missing diretory services server address.");
            }
        }

        private string DirSvcUser
        {
            get
            {
                var configValue = this.UseAD ? Manager.GetApplicationConfigValue("ADUser", DirSvcNameSpace) : Manager.GetApplicationConfigValue("eDirUser", DirSvcNameSpace);

                if (null != configValue)
                    return configValue.ToString().Trim();
                else
                    throw new Exception("Missing diretory services user ID.");
            }
        }

        private string DirSvcPassword
        {
            get
            {
                var configValue = this.UseAD ? Manager.GetApplicationConfigValue("ADPassword", DirSvcNameSpace) : Manager.GetApplicationConfigValue("eDirPassword", DirSvcNameSpace);

                if (null != configValue)
                    return configValue.ToString().Trim();
                else
                    throw new Exception("Missing diretory services server password.");
            }
        }

        private bool DirSvcCertAcceptAll
        {
            get
            {
                var configValue = this.UseAD ? Manager.GetApplicationConfigValue("ADCertificateAcceptAll", DirSvcNameSpace) : Manager.GetApplicationConfigValue("eDirCertificateAcceptAll", DirSvcNameSpace);

                if (null != configValue)
                    return string.Equals(configValue.ToString().Trim(), "true", StringComparison.InvariantCultureIgnoreCase);
                else
                    throw new Exception("Missing diretory services certificate acceptance policy.");
            }
        }

        private string DirSvcCertPath
        {
            get
            {
                var configValue = this.UseAD ? Manager.GetApplicationConfigValue("ADCertificatePath", DirSvcNameSpace) : Manager.GetApplicationConfigValue("eDirCertificatePath", DirSvcNameSpace);

                if (null != configValue)
                    return configValue.ToString().Trim();
                else
                    return "";
            }
        }

        private string SearchObject
        {
            get
            {
                return this.UseAD ? "objectCategory=user" : "objectClass=Person";
            }
        }

        private LdapConnection ldapConnection = null;

        private void AuthenticateLDAP()
        {

            try
            {
                ldapConnection = new LdapConnection(new LdapDirectoryIdentifier(DirSvcServer));
                ldapConnection.SessionOptions.SecureSocketLayer = true;
                ldapConnection.SessionOptions.RootDseCache = true;
                ldapConnection.SessionOptions.VerifyServerCertificate = new VerifyServerCertificateCallback(ServerCallback);
                ldapConnection.Credential = new NetworkCredential(DirSvcUser, DirSvcPassword);
                ldapConnection.AuthType = AuthType.Basic;
                ldapConnection.Bind();
            }
            catch
            {
                if (null != ldapConnection)
                    ldapConnection.Dispose();

                throw;
            }
        }
        private bool ServerCallback(LdapConnection connection, X509Certificate certificate)
        {
            if (DirSvcCertAcceptAll)
                return true;

            string _strPath = AppDomain.CurrentDomain.BaseDirectory + (DirSvcCertPath);
            try
            {
                X509Certificate expectedCert = X509Certificate.CreateFromCertFile(_strPath);

                return expectedCert.Equals(certificate);
            }
            catch
            {
                return false;
            }
        }

        public SearchResponse GetResults(string Filter)
        {
            try
            {
                AuthenticateLDAP();
                SearchRequest request = new SearchRequest(DistinguishedNameForSearch, Filter, SearchScope.Subtree);
                return (SearchResponse)ldapConnection.SendRequest(request);
            }
            finally
            {
                if (null != ldapConnection)
                    ldapConnection.Dispose();
            }
        }

        public MyCustomMembershipUsers[] LDAP_FindUser(string SearchText, LDAP_FindUserOptions Option)
        {
                string strFilter = string.Empty;

                switch (Option)
                {
                    case LDAP_FindUserOptions.username:
                        strFilter = "(&(" + this.SearchObject + ")(uid=*" + SearchText + "*))";
                        break;
                    case LDAP_FindUserOptions.firstname:
                        strFilter = "(&(" + this.SearchObject + ")(givenName=*" + SearchText + "*))";
                        break;
                    case LDAP_FindUserOptions.lastname:
                        strFilter = "(&(" + this.SearchObject + ")(sn=*" + SearchText + "*))";
                        break;
                    case LDAP_FindUserOptions.email:
                        strFilter = "(&(" + this.SearchObject + ")(mail=*" + SearchText + "*))";
                        break;
                    default:
                        throw new ArgumentException("Unsupported search option.");
                }

                SearchResponse results = GetResults(strFilter);

                if (null == results || null == results.Entries || results.Entries.Count < 1)
                    return new MyCustomMembershipUsers[0];

                MyCustomMembershipUsers[] objReturn = new MyCustomMembershipUsers[results.Entries.Count];
                int Count = 0;

                foreach (SearchResultEntry CurUser in results.Entries)
                {
                    objReturn[Count] = new MyCustomMembershipUsers(CurUser);
                    Count++;
                }

                return objReturn;
        }
    }

    public enum LDAP_FindUserOptions
    {
        username,
        firstname,
        lastname,
        email
    }
}


