using System.Configuration;
using System.Data.SqlClient;

namespace NBO.Test.Framework
{
    public class SqlConnectionProvider : ISqlTestConnectionProvider
    {
        protected SqlConnectionStringBuilder ConnectionString { get; private set; }

        /********
                private string ConnectionFile { get; }

                public SqlConnectionProvider(): this(ConfigurationManager.AppSettings["connfile"])
                {
                }

                public SqlConnectionProvider(string connectionFile)
                {
                    ConnectionFile = connectionFile;
                }

                protected virtual void Initialize()
                {
                    ConnectionString = new SqlConnectionStringBuilder()
                    {
                        ConnectionString = MenuLink.Tools.Data.Utils.ReadConnectionString(ConnectionFile)
                    };
                }
        **************/

        public SqlConnectionProvider(string connectionStringParam)
        {
            ConnectionString = new SqlConnectionStringBuilder()
            {
                ConnectionString = connectionStringParam
            };
        }


        public string GetConnectionString(string connectionId)
        {
            return ConnectionString.ConnectionString;
        }

        public string GetDatabaseName()
        {
            return $@"{ConnectionString.DataSource}\{ConnectionString.InitialCatalog}";
        }

        public string GetInitialCatalog()
        {
            return $@"{ConnectionString.InitialCatalog}";
        }
    }
}
