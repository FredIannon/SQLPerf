// Change History
// ======================================
// 12/08/17 LKR - NBO-5953 Populate the Intermediate Table with Menu Item Site Item Name Change
// 11/11/15 LKR - WI 43345 Web Services: Invoice Push: Polling procedure, dequeue procedure

namespace NBO.Test.Framework
{
    /// <summary>
    /// Connection string provider, by database ConnectinId
    /// </summary>
    public interface ISqlTestConnectionProvider
    {
        string GetConnectionString(string connectionId);
        string GetDatabaseName();
        string GetInitialCatalog();
    }
}
