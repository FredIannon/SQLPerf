
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Ajax.Utilities;         // Weird this is from WebGrease
using Microsoft.VisualBasic.FileIO;

namespace NBO.Test.Framework
{
    public interface ISqlExecutor
    {
        int ExecSql(string sql, bool consoleLog = false);
        Exception ExecSqlEx(string sql, bool consoleLog = false);
        int ExecSqlWithInjection(string sql, string containerName, string injectionContent);
        void ExecSqlT(string sql);
        T ExecSql<T>(string sql);
        List<T> ExecSqlList<T>(string sql);
        List<T> ExecSqlList<T>(string sql, Func<object[], T> map);
        void ExecSqlBatch(string sql, bool consoleLog = false);
        DataSet Fill(string sql);
        void BulkInsert(DataTable source, string tableName, Dictionary<string, string> extensions = null);
        Dictionary<string, string> BuildDefaultCollationSet(DataTable dt);
        void CreateDatabaseSnapshot();
        void RevertDatabaseFromSnapshot();
        string RetrieveFunctionCodeAsProc(string name);
        string RetrieveProcedureCode(string name, bool alterProcedure = false);
        string RetrieveViewCodeAsProc(string name);
        IList<StoredProc> GetProcedures();
        IList<StoredProc> GetReadOnlyProcedures();
        IList<StoredProc> GetDependentProcedures(int procedureId);
        IList<StoredProc> GetDependentProcedures(string procedureName);
        IList<StoredProc> GetDependentProceduresRec(int procedureId);
        IList<StoredProc> GetDependentProceduresRec(string procedureName);
        ReadOnlyFlag GetProcedureReadOnlyFlag(string procedureName);
        void DropProcedureOrFunction(string name);
        SqlParameterCollection GetProcParameters(string procedureName);
        string GetProcParametersAsTSql(string procedureName);
        string GetProcDummyParameterValues(string procedureName);
        bool IsFunction(string procedureName);
        SqlEntityType GetObjectType(string objectName);
        void RunSqlFile(string fullPath, bool consoleLog = false);
        Exception RunSqlFileEx(string fullPath, bool consoleLog = false);
        void Execute(Action<SqlConnection> code);
        void FreeProcCache();
        void CollectQueryStatistics(bool truncate, string taskName);
        void CollectQueryStatistics(bool truncate, string taskName, string targetDatabase, string targetTable);
        void CollectProcStatistics(bool truncateQueryStats, string taskName);
        string RetrieveCode(string objectName);
        void CollectProcStatistics(bool truncate, string taskName, string targetDatabase, string targetTable);
        void CollectIndexStatistics(string taskName);
        void CollectIndexStatistics(string targetDatabase, string targetTable, string taskName);
        void CollectIndexMissingStatistics(string taskName);
        void CollectIndexMissingStatistics(string targetDatabase, string targetTable, string taskName);
        void DeleteUnchangedIndexMissingStatistic(string endTaskName, string startTaskName);
        void DeleteUnchangedIndexStatistic(string endTaskName, string startTaskName);
        void TruncateQueryStatistics();
        void TruncateQueryStatistics(string targetDatabase, string targetTable);
        void TruncateProcStatistics();
        void TruncateProcStatistics(string targetDatabase, string targetTable);
        void TruncateIndexStatistics();
        void TruncateIndexStatistics(string targetDatabase, string targetTable);
        void TruncateIndexMissingStatistics();
        void TruncateIndexMissingStatistics(string targetDatabase, string targetTable);
        DataTable ReadCsvFile(string fileName, string delimiter);
        void WriteCsvFile(DataTable dt, string fileName, string delimiter);
    }

    public class SqlExecutor : ISqlExecutor
    {
        public ISqlTestConnectionProvider ConnectionProvider { get; set; }

        private const string SNAPSHOT_NAME = "ml_testSnapshot";
        private const string DATABASE_NAME = "Trace";
        private const string TABLE_PROCS = "__TestResultsProcs";
        private const string TABLE_QUERIES = "__TestResultsQueries";
        private const string TABLE_INDEXES = "__TestResultsIndexes";
        private const string TABLE_INDEXESMISSING = "__TestResultsIndexesMissing";
        private const string CONNECTION_ID = null;
        private SqlConnection _connection = null;
        private readonly Dictionary<string, ReadOnlyFlag> _procedureFlags = new Dictionary<string, ReadOnlyFlag>();
        private readonly Dictionary<string, IList<StoredProc>> _procedureDependenciesRec = new Dictionary<string, IList<StoredProc>>();
        private readonly Dictionary<string, SqlParameterCollection> _procParametersCache = new Dictionary<string, SqlParameterCollection>();

        public SqlExecutor(ISqlTestConnectionProvider connectionProvider)
        {
            ConnectionProvider = connectionProvider;
        }

        /// <summary>
        /// Executes batch of SQL queries divided by GO
        /// </summary>
        public void ExecSqlBatch(string sql, bool consoleLog = false)
        {
            sql.Split(new[] { "GO\r\n" }, StringSplitOptions.RemoveEmptyEntries).ForEach(s => ExecSql(s, consoleLog));
        }

        /// <summary>
        /// Executes SQL command and returns number of rows affected
        /// </summary>
        public int ExecSql(string sql, bool consoleLog = false)
        {
            if (_connection != null)
            {
                return ExecSql(sql, _connection, consoleLog);
            }
            using (var connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID)))
            {
                connection.Open();
                return ExecSql(sql, connection, consoleLog);
            }
        }

        public Exception ExecSqlEx(string sql, bool consoleLog = false)
        {
            try
            {
                ExecSql(sql, consoleLog);
            }
            catch (Exception ex)
            {
                return ex;
            }
            return null;
        }

        private int ExecSql(string sql, SqlConnection connection, bool consoleLog)
        {
            var command = connection.CreateCommand();
            command.CommandTimeout = 360000;
            command.CommandText = sql;
            if (consoleLog)
            {
                connection.InfoMessage += (s, m) =>
                    {
                        Console.WriteLine("SQL: " + m.Message);
                    };
            }

            try
            {
                return command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }

        /// <summary>
        /// Applies code injection and executes the command.
        /// </summary>
        public int ExecSqlWithInjection(string sql, string containerName, string injectionContent)
        {
            var name = Regex.Escape(containerName);
            return ExecSql(new Regex(@"(?<=/\*" + name + @"\*/)([\s\S]*)(?=/\*/" + name + @"\*/)").Replace(sql, injectionContent));
        }

        /// <summary>
        /// Executes SQL command in a transaction.
        /// </summary>
        public void ExecSqlT(string sql)
        {
            using (var connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID)))
            {
                connection.Open();
                using (var trans = connection.BeginTransaction())
                {
                    var command = connection.CreateCommand();
                    command.CommandText = sql;
                    command.Transaction = trans;
                    command.ExecuteNonQuery();
                    trans.Commit();
                }
            }
        }

        public T ExecSql<T>(string sql)
        {
            using (var connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID)))
            {
                connection.Open();
                return ExecSql<T>(sql, connection);
            }
        }

        private T ExecSql<T>(string sql, SqlConnection connection)
        {
            var command = connection.CreateCommand();
            command.CommandText = sql;
            var res = command.ExecuteScalar();
            if (res == DBNull.Value || res == null)
            {
                return default(T);
            }
            return (T)res;
        }

        public List<T> ExecSqlList<T>(string sql)
        {
            var res = new List<T>();
            using (var connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID)))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = sql;
                var reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        res.Add((T)reader.GetValue(0));
                    }
                }
            }
            return res;
        }

        public List<T> ExecSqlList<T>(string sql, Func<object[], T> mapping)
        {
            var res = new List<T>();
            using (var connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID)))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = sql;
                var reader = command.ExecuteReader();
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        var values = new object[reader.FieldCount];
                        reader.GetValues(values);
                        res.Add(mapping(values));
                    }
                }
            }
            return res;
        }

        public DataSet Fill(string sql)
        {
            if (_connection != null)
            {
                return Fill(sql, _connection);
            }
            using (var connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID)))
            {
                connection.Open();
                return Fill(sql, connection);
            }
        }

        public DataTable ReadCsvFile(string fileName, string delimiter)
        {
            var dt = new DataTable();
            using (var reader = new TextFieldParser(fileName))
            {
                reader.SetDelimiters(delimiter);
                reader.HasFieldsEnclosedInQuotes = false;
                //build schema
                var fields = reader.ReadFields();
                if (fields == null)
                {
                    throw new Exception(string.Format("File '{0}' contains no fields.", fileName));
                }
                foreach (var column in fields)
                {
                    dt.Columns.Add(new DataColumn(column) { AllowDBNull = true });
                }
                //read data
                while (!reader.EndOfData)
                {
                    dt.Rows.Add(reader.ReadFields());
                }
            }
            return dt;
        }

        public void WriteCsvFile(DataTable dt, string fileName, string delimiter)
        {
            var sb = new StringBuilder();
            //write column names
            var columnNames = dt.Columns.Cast<DataColumn>().Select(column => column.ColumnName);
            sb.AppendLine(string.Join(delimiter, columnNames));
            //write data
            foreach (DataRow row in dt.Rows)
            {
                var fields = row.ItemArray.Select(field => field.ToString());
                sb.AppendLine(string.Join(delimiter, fields));
            }
            //flush to file
            File.WriteAllText(fileName, sb.ToString());
        }

        private DataSet Fill(string sql, SqlConnection connection)
        {
            var result = new DataSet();
            using (var da = new SqlDataAdapter(sql, connection))
            {
                da.Fill(result);
            }
            return result;
        }

        public void BulkInsert(DataTable source, string tableName, Dictionary<string, string> collations = null)
        {
            if (_connection != null)
            {
                BulkInsert(source, tableName, collations, _connection);
            }
            else
            {
                using (var connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID)))
                {
                    connection.Open();
                    BulkInsert(source, tableName, collations, connection);
                }
            }
        }

        private void BulkInsert(DataTable source, string tableName, Dictionary<string, string> extensions, SqlConnection connection)
        {
            ExecSql(SqlTableCreator.GetCreateFromDataTableSQL(tableName, source, extensions), connection, false);

            var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null)
            {
                DestinationTableName = tableName,
                BatchSize = source.Rows.Count
            };
            bulkCopy.WriteToServer(source);
            bulkCopy.Close();
        }

        /// <summary>
        /// Builds a collection of extensions for bulk insert method. For all string columns in the provided DataTable creates a single item: [ColumnName], COLLATION DATABASE_DEFAULT
        /// </summary>
        public Dictionary<string, string> BuildDefaultCollationSet(DataTable dt)
        {
            var collations = new Dictionary<string, string>();
            dt.Columns.Cast<DataColumn>().Where(c => c.DataType == typeof(string)).ForEach(c => collations.Add(c.ColumnName, "COLLATE DATABASE_DEFAULT"));
            return collations;
        }

        public void CreateDatabaseSnapshot()
        {

            var dataFileName = ExecSql<string>("SELECT name FROM sys.master_files WHERE database_id = DB_ID() AND type_desc = 'ROWS'");
            var databaseName = ExecSql<string>("SELECT DB_NAME()");
            var backupPath = ExecSql<string>("DECLARE @BackupDirectory varchar(1000); EXEC master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE', N'Software\\Microsoft\\MSSQLServer\\MSSQLServer',N'BackupDirectory',@BackupDirectory OUTPUT; SELECT @BackupDirectory");

            ExecSql(
                "CREATE DATABASE " + SNAPSHOT_NAME + " " +
                "ON ( NAME = '" + dataFileName + "', FILENAME = '" + Path.Combine(backupPath, SNAPSHOT_NAME) + "' ) " +
                "AS SNAPSHOT OF [" + databaseName + "];");
        }

        public void RevertDatabaseFromSnapshot()
        {
            var databaseName = ExecSql<string>("SELECT DB_NAME()");
            try
            {
                ExecSql(
                    "CHECKPOINT; USE master; " +
                    "ALTER DATABASE [" + databaseName + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; " +
                    "RESTORE DATABASE [" + databaseName + "] FROM DATABASE_SNAPSHOT = '" + SNAPSHOT_NAME + "'; DROP DATABASE " + SNAPSHOT_NAME + "; " +
                    "USE [" + databaseName + "]; "
                );
            }
            finally
            {
                ExecSql("ALTER DATABASE [" + databaseName + "] SET MULTI_USER WITH ROLLBACK IMMEDIATE; ");
            }

        }

        public void FreeProcCache()
        {
            ExecSql("CHECKPOINT; DBCC DROPCLEANBUFFERS; DBCC FREEPROCCACHE;");
        }

        public void DeleteUnchangedIndexMissingStatistic(string endTaskName, string startTaskName)
        {
            var sql = new StringBuilder();

            sql.AppendLine("DELETE s FROM " + DATABASE_NAME + ".[dbo]." + TABLE_INDEXESMISSING + " s, " + DATABASE_NAME + ".[dbo]." +
                           TABLE_INDEXESMISSING + " o WHERE s.test = '" + endTaskName + "' and o.test = '" + startTaskName + "' "
                           + "AND o.userSeeks = s.userSeeks AND o.userScans = s.userScans AND o.avgUserCost = s.avgUserCost AND o.avgUserImpact = s.avgUserImpact AND ISNULL(o.tableName, '') = ISNULL(s.tableName, '') AND ISNULL(o.equalityColumns, '') = ISNULL(s.equalityColumns, '') AND ISNULL(o.includedColumns, '') = ISNULL(s.includedColumns, '')");

            ExecSql(sql.ToString());


            ExecSql("DELETE FROM " + DATABASE_NAME + ".[dbo]." + TABLE_INDEXESMISSING + " WHERE test = '" + startTaskName + "'");
        }

        public void DeleteUnchangedIndexStatistic(string endTaskName, string startTaskName)
        {
            var sql = new StringBuilder();

            sql.AppendLine("DELETE s FROM " + DATABASE_NAME + ".[dbo]." + TABLE_INDEXES + " s, " + DATABASE_NAME + ".[dbo]." +
                           TABLE_INDEXES + " o WHERE s.test = '" + endTaskName + "' and o.test = '" +
                           startTaskName +
                           "' AND o.indexID = s.indexID AND o.userSeeks = s.userSeeks AND o.userScans = s.userScans AND o.userLookups = s.userLookups AND o.userUpdates = s.userUpdates");

            ExecSql(sql.ToString());

            ExecSql("DELETE FROM " + DATABASE_NAME + ".[dbo]." + TABLE_INDEXES + " WHERE test = '" + startTaskName + "'");
        }

        public void CollectProcStatistics(bool truncate, string taskName)
        {
            CollectProcStatistics(truncate, taskName, DATABASE_NAME, TABLE_PROCS);
        }

        public void CollectProcStatistics(bool truncate, string taskName, string targetDatabase, string targetTable)
        {
            var sql = new StringBuilder();

            sql.AppendLine("IF  NOT EXISTS (SELECT * FROM " + targetDatabase + ".sys.objects WHERE object_id = OBJECT_ID(N'" + targetDatabase + ".[dbo]." + targetTable + "') AND type in (N'U')) ");
            sql.AppendLine("BEGIN ");
            sql.AppendLine("  CREATE TABLE " + targetDatabase + ".[dbo]." + targetTable + "( ");
            sql.AppendLine("    [test] [nvarchar](200) NOT NULL, ");
            sql.AppendLine("    [name] [nvarchar](200), ");
            sql.AppendLine("    [total_elapsed_time_SECONDS] [float] NOT NULL, ");
            sql.AppendLine("    [avg_physical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [execution_count] [int] NOT NULL, ");
            sql.AppendLine("    [total_elapsed_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [total_cpu_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [total_logical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [total_logical_writes] [bigint] NOT NULL, ");
            sql.AppendLine("    [avg_elapsed_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_elapsed_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_elapsed_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [avg_cpu_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_worker_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_worker_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [avg_logical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_logical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_logical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_physical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_physical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [avg_logical_writes] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_logical_writes] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_logical_writes] [bigint] NOT NULL, ");
            sql.AppendLine("    [plan_count] [int] NOT NULL");
            sql.AppendLine("  ) ON [PRIMARY]");
            sql.AppendLine("END ");
            if (truncate)
            {
                sql.AppendLine("ELSE TRUNCATE TABLE " + targetDatabase + ".dbo." + targetTable + " ");
            }
            sql.AppendLine("INSERT INTO " + targetDatabase + ".dbo." + targetTable + " (");
            sql.AppendLine("    [test], ");
            sql.AppendLine("    [name], ");
            sql.AppendLine("    [total_elapsed_time_SECONDS], ");
            sql.AppendLine("    [avg_physical_reads], ");
            sql.AppendLine("    [execution_count], ");
            sql.AppendLine("    [total_elapsed_time], ");
            sql.AppendLine("    [total_cpu_time], ");
            sql.AppendLine("    [total_logical_reads], ");
            sql.AppendLine("    [total_logical_writes], ");
            sql.AppendLine("    [avg_elapsed_time], ");
            sql.AppendLine("    [min_elapsed_time], ");
            sql.AppendLine("    [max_elapsed_time], ");
            sql.AppendLine("    [avg_cpu_time], ");
            sql.AppendLine("    [min_worker_time], ");
            sql.AppendLine("    [max_worker_time], ");
            sql.AppendLine("    [avg_logical_reads], ");
            sql.AppendLine("    [min_logical_reads], ");
            sql.AppendLine("    [max_logical_reads], ");
            sql.AppendLine("    [min_physical_reads], ");
            sql.AppendLine("    [max_physical_reads], ");
            sql.AppendLine("    [avg_logical_writes], ");
            sql.AppendLine("    [min_logical_writes], ");
            sql.AppendLine("    [max_logical_writes], ");
            sql.AppendLine("    [plan_count] ");
            sql.AppendLine(") ");
            sql.AppendLine("SELECT ");
            sql.AppendLine("  '" + taskName + "', ");
            sql.AppendLine("  o.name,  ");
            sql.AppendLine("  SUM(s.total_elapsed_time) / 1000000.0 [total_elapsed_time_SECONDS], ");
            sql.AppendLine("  SUM(s.total_physical_reads)/SUM(s.execution_count) [avg_physical_reads], ");
            sql.AppendLine("  SUM(s.execution_count) execution_count, ");
            sql.AppendLine("  SUM(s.total_elapsed_time) [total_elapsed_time], ");
            sql.AppendLine("  SUM(s.total_worker_time) [total_cpu_time], ");
            sql.AppendLine("  SUM(s.total_logical_reads) [total_logical_reads], ");
            sql.AppendLine("  SUM(s.total_logical_writes) [total_logical_writes], ");
            sql.AppendLine("  SUM(s.total_elapsed_time)/SUM(s.execution_count) [avg_elapsed_time], ");
            sql.AppendLine("  MIN(s.min_elapsed_time) min_elapsed_time,    ");
            sql.AppendLine("  MAX(s.max_elapsed_time) max_elapsed_time, ");
            sql.AppendLine("  SUM(s.total_worker_time)/SUM(s.execution_count) [avg_cpu_time], ");
            sql.AppendLine("  MIN(s.min_worker_time) min_worker_time, ");
            sql.AppendLine("  MAX(s.max_worker_time) max_worker_time, ");
            sql.AppendLine("  SUM(s.total_logical_reads)/SUM(s.execution_count) [avg_logical_reads], ");
            sql.AppendLine("  MIN(s.min_logical_reads) min_logical_reads, ");
            sql.AppendLine("  MAX(s.max_logical_reads) max_logical_reads, ");
            sql.AppendLine("  MIN(s.min_physical_reads) min_physical_reads, ");
            sql.AppendLine("  MAX(s.max_physical_reads) max_physical_reads, ");
            sql.AppendLine("  SUM(s.total_logical_writes)/SUM(s.execution_count) [avg_logical_writes], ");
            sql.AppendLine("  MIN(s.min_logical_writes) min_logical_writes, ");
            sql.AppendLine("  MAX(s.max_logical_writes) max_logical_writes, ");
            sql.AppendLine("  COUNT(DISTINCT plan_handle) plan_count ");
            sql.AppendLine("FROM sys.dm_exec_procedure_stats AS s ");
            sql.AppendLine("LEFT JOIN sys.objects AS o ");
            sql.AppendLine("ON s.[object_id] = o.[object_id] ");
            sql.AppendLine("GROUP BY o.name ");
            ExecSql(sql.ToString());
        }

        public void CollectQueryStatistics(bool truncate, string taskName)
        {
            CollectQueryStatistics(truncate, taskName, DATABASE_NAME, TABLE_QUERIES);
        }

        public void CollectQueryStatistics(bool truncate, string taskName, string targetDatabase, string targetTable)
        {
            var sql = new StringBuilder();

            sql.AppendLine("IF  NOT EXISTS (SELECT * FROM " + targetDatabase + ".sys.objects WHERE object_id = OBJECT_ID(N'" + targetDatabase + ".[dbo]." + targetTable + "') AND type in (N'U')) ");
            sql.AppendLine("BEGIN ");
            sql.AppendLine("  CREATE TABLE " + targetDatabase + ".[dbo]." + targetTable + "( ");
            sql.AppendLine("    [test] [nvarchar](200) NOT NULL, ");
            sql.AppendLine("    [name] [nvarchar](200), ");
            sql.AppendLine("    [offsetStart] int NOT NULL, ");
            sql.AppendLine("    [offsetEnd] int NOt NULL, ");
            sql.AppendLine("    [xmlPlan] [xml], ");
            sql.AppendLine("    [execution_count] [int] NOT NULL, ");
            sql.AppendLine("    [total_elapsed_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [total_cpu_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [total_logical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [total_logical_writes] [bigint] NOT NULL, ");
            sql.AppendLine("    [avg_elapsed_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_elapsed_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_elapsed_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [avg_cpu_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_worker_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_worker_time] [bigint] NOT NULL, ");
            sql.AppendLine("    [avg_logical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_logical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_logical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [avg_physical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_physical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_physical_reads] [bigint] NOT NULL, ");
            sql.AppendLine("    [avg_logical_writes] [bigint] NOT NULL, ");
            sql.AppendLine("    [min_logical_writes] [bigint] NOT NULL, ");
            sql.AppendLine("    [max_logical_writes] [bigint] NOT NULL, ");
            sql.AppendLine("    [statement] nvarchar(max), ");
            sql.AppendLine("    [creation_time] datetime, ");
            sql.AppendLine("    [last_execution_time] datetime ");
            sql.AppendLine("  ) ON [PRIMARY]");
            sql.AppendLine("END ");
            if (truncate)
            {
                sql.AppendLine("ELSE TRUNCATE TABLE " + targetDatabase + ".dbo." + targetTable + " ");
            }
            sql.AppendLine("INSERT INTO " + targetDatabase + ".dbo." + targetTable + " (");
            sql.AppendLine("    [test], ");
            sql.AppendLine("    [name], ");
            sql.AppendLine("    [offsetStart], ");
            sql.AppendLine("    [offsetEnd], ");
            sql.AppendLine("    [xmlPlan], ");
            sql.AppendLine("    [execution_count], ");
            sql.AppendLine("    [total_elapsed_time], ");
            sql.AppendLine("    [total_cpu_time], ");
            sql.AppendLine("    [total_logical_reads], ");
            sql.AppendLine("    [total_logical_writes], ");
            sql.AppendLine("    [avg_elapsed_time], ");
            sql.AppendLine("    [min_elapsed_time], ");
            sql.AppendLine("    [max_elapsed_time], ");
            sql.AppendLine("    [avg_cpu_time], ");
            sql.AppendLine("    [min_worker_time], ");
            sql.AppendLine("    [max_worker_time], ");
            sql.AppendLine("    [avg_logical_reads], ");
            sql.AppendLine("    [min_logical_reads], ");
            sql.AppendLine("    [max_logical_reads], ");
            sql.AppendLine("    [avg_physical_reads], ");
            sql.AppendLine("    [min_physical_reads], ");
            sql.AppendLine("    [max_physical_reads], ");
            sql.AppendLine("    [avg_logical_writes], ");
            sql.AppendLine("    [min_logical_writes], ");
            sql.AppendLine("    [max_logical_writes], ");
            sql.AppendLine("    [statement], ");
            sql.AppendLine("    [creation_time], ");
            sql.AppendLine("    [last_execution_time] ");
            sql.AppendLine(") ");
            sql.AppendLine("SELECT ");
            sql.AppendLine("  '" + taskName + "', ");
            sql.AppendLine("  o.name,                                                            ");
            sql.AppendLine("  qs.statement_start_offset /2 +1 [offsetStart],                     ");
            sql.AppendLine("  qs.statement_end_offset/2+1 [offsetEnd],                           ");
            sql.AppendLine("  CAST(qp.query_plan as xml) [xmlPlan],                              ");
            sql.AppendLine("  qs.execution_count,                                                ");
            sql.AppendLine("  qs.total_elapsed_time,                                             ");
            sql.AppendLine("  qs.total_worker_time [total_cpu_time],                             ");
            sql.AppendLine("  qs.total_logical_reads,                                            ");
            sql.AppendLine("  qs.total_logical_writes,                                            ");
            sql.AppendLine("  qs.total_elapsed_time/qs.execution_count [avg_elapsed_time],       ");
            sql.AppendLine("  qs.min_elapsed_time,                                               ");
            sql.AppendLine("  qs.max_elapsed_time,                                               ");
            sql.AppendLine("  qs.total_worker_time/qs.execution_count [avg_cpu_time],            ");
            sql.AppendLine("  qs.min_worker_time [min_cpu_time],                                 ");
            sql.AppendLine("  qs.max_worker_time [max_cpu_time],                                 ");
            sql.AppendLine("  qs.total_logical_reads/qs.execution_count [avg_logical_reads],     ");
            sql.AppendLine("  qs.min_logical_reads,                                              ");
            sql.AppendLine("  qs.max_logical_reads,                                              ");
            sql.AppendLine("  qs.total_physical_reads/qs.execution_count [avg_physical_reads],     ");
            sql.AppendLine("  qs.min_physical_reads,                                              ");
            sql.AppendLine("  qs.max_physical_reads,                                              ");
            sql.AppendLine("  qs.total_logical_writes/qs.execution_count [avg_logical_writes],     ");
            sql.AppendLine("  qs.min_logical_writes,                                              ");
            sql.AppendLine("  qs.max_logical_writes,                                              ");
            sql.AppendLine("  SUBSTRING(st.text, (qs.statement_start_offset/2) + 1,((CASE statement_end_offset WHEN -1 then DATALENGTH(st.text) ELSE qs.statement_end_offset END - qs.statement_start_offset)/2) + 1) as statement_text,  ");
            sql.AppendLine("  qs.last_execution_time,                                            ");
            sql.AppendLine("  qs.creation_time ");
            sql.AppendLine("FROM sys.dm_exec_query_stats qs ");
            sql.AppendLine("CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st ");
            sql.AppendLine("CROSS APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset, qs.statement_end_offset) qp ");
            sql.AppendLine("LEFT JOIN sys.objects o ");
            sql.AppendLine("  ON o.object_id = qp.objectid ");
            sql.AppendLine("WHERE qp.dbid = DB_ID() ");
            sql.AppendLine("ORDER BY o.name, qs.[total_logical_reads] DESC ");
            sql.AppendLine("OPTION (RECOMPILE) ");
            ExecSql(sql.ToString());
        }


        public void CollectIndexStatistics(string taskName)
        {
            CollectIndexStatistics(DATABASE_NAME, TABLE_INDEXES, taskName);
        }


        public void CollectIndexStatistics(string targetDatabase, string targetTable, string taskName)
        {
            var sql = new StringBuilder();

            sql.AppendLine("IF  NOT EXISTS (SELECT * FROM " + targetDatabase + ".sys.objects WHERE object_id = OBJECT_ID(N'" + targetDatabase + ".[dbo]." + targetTable + "') AND type in (N'U')) ");
            sql.AppendLine("BEGIN ");
            sql.AppendLine("  CREATE TABLE " + targetDatabase + ".[dbo]." + targetTable + "( ");
            sql.AppendLine("     [test] [nvarchar](200) NOT NULL,                  ");
            sql.AppendLine("     [tableName] [nvarchar](200) NOT NULL, ");
            sql.AppendLine("     indexID int NOT NULL, ");
            sql.AppendLine("     indexName [nvarchar](200), ");
            sql.AppendLine("     userSeeks int, ");
            sql.AppendLine("     userScans int, ");
            sql.AppendLine("     userLookups int, ");
            sql.AppendLine("     userUpdates int, ");
            sql.AppendLine("     timeStamp datetime ");
            sql.AppendLine("  ) ON [PRIMARY]");
            sql.AppendLine("END ");
            sql.AppendLine("INSERT INTO " + targetDatabase + ".dbo." + targetTable + " (");
            sql.AppendLine("     [test],                     ");
            sql.AppendLine("     [tableName], ");
            sql.AppendLine("     indexID, ");
            sql.AppendLine("     indexName, ");
            sql.AppendLine("     userSeeks, ");
            sql.AppendLine("     userScans, ");
            sql.AppendLine("     userLookups, ");
            sql.AppendLine("     userUpdates, ");
            sql.AppendLine("     timeStamp ");
            sql.AppendLine(") ");
            sql.AppendLine("SELECT ");
            sql.AppendLine("    '" + taskName + "',                     ");
            sql.AppendLine("  OBJECT_NAME(s.object_id), ");
            sql.AppendLine("  s.index_id, ");
            sql.AppendLine("  i.[name], ");
            sql.AppendLine("  s.user_seeks, ");
            sql.AppendLine("  s.user_scans, ");
            sql.AppendLine("  s.user_lookups, ");
            sql.AppendLine("  s.user_updates, ");
            sql.AppendLine("  GETUTCDATE() ");
            sql.AppendLine("FROM sys.dm_db_index_usage_stats s WITH (NOLOCK) ");
            sql.AppendLine("INNER JOIN sys.indexes i ");
            sql.AppendLine("  ON i.index_id = s.index_id ");
            sql.AppendLine("  AND i.object_id = s.object_id ");
            sql.AppendLine("WHERE s.database_id = DB_ID()     ");
            ExecSql(sql.ToString());
        }

        public void CollectIndexMissingStatistics(string taskName)
        {
            CollectIndexMissingStatistics(DATABASE_NAME, TABLE_INDEXESMISSING, taskName);
        }

        public void CollectIndexMissingStatistics(string targetDatabase, string targetTable, string taskName)
        {
            var sql = new StringBuilder();

            sql.AppendLine("IF  NOT EXISTS (SELECT * FROM " + targetDatabase + ".sys.objects WHERE object_id = OBJECT_ID(N'" + targetDatabase + ".[dbo]." + targetTable + "') AND type in (N'U')) ");
            sql.AppendLine("BEGIN ");
            sql.AppendLine("  CREATE TABLE " + targetDatabase + ".[dbo]." + targetTable + "( ");
            sql.AppendLine("    [test] [nvarchar](200) NOT NULL,                  ");
            sql.AppendLine("    [indexHandle] [int] NOT NULL,                  ");
            sql.AppendLine("    [tableName] [nvarchar](200) NOT NULL,          ");
            sql.AppendLine("    [equalityColumns] [nvarchar](4000) NULL,       ");
            sql.AppendLine("    [inequalityColumns] [nvarchar](4000) NULL,     ");
            sql.AppendLine("    [includedColumns] [nvarchar](4000) NULL,       ");
            sql.AppendLine("    [uniqueCompiles] [bigint] NOT NULL,            ");
            sql.AppendLine("    [userSeeks] [bigint] NOT NULL,                 ");
            sql.AppendLine("    [userScans] [bigint] NOT NULL,                 ");
            sql.AppendLine("    [lastSeek] [datetime] NULL,                    ");
            sql.AppendLine("    [lastScan] [datetime] NULL,                    ");
            sql.AppendLine("    [avgUserCost] [float] NULL,                    ");
            sql.AppendLine("    [avgUserImpact] [float] NULL                   ");
            sql.AppendLine(") ");
            sql.AppendLine("END ");
            sql.AppendLine("INSERT INTO " + targetDatabase + ".dbo." + targetTable + " (");
            sql.AppendLine("      [test],                     ");
            sql.AppendLine("      [indexHandle],                     ");
            sql.AppendLine("      [tableName],                       ");
            sql.AppendLine("      [equalityColumns],                 ");
            sql.AppendLine("      [inequalityColumns],               ");
            sql.AppendLine("      [includedColumns],                 ");
            sql.AppendLine("      [uniqueCompiles],                  ");
            sql.AppendLine("      [userSeeks],                       ");
            sql.AppendLine("      [userScans],                       ");
            sql.AppendLine("      [lastSeek],                        ");
            sql.AppendLine("      [lastScan],                        ");
            sql.AppendLine("      [avgUserCost],                     ");
            sql.AppendLine("      [avgUserImpact]                    ");
            sql.AppendLine("    )                                    ");
            sql.AppendLine("  SELECT                                 ");
            sql.AppendLine("    '" + taskName + "',                     ");
            sql.AppendLine("    mid.index_handle,                    ");
            sql.AppendLine("    OBJECT_NAME(mid.[object_id]),        ");
            sql.AppendLine("    mid.equality_columns,                ");
            sql.AppendLine("    mid.inequality_columns,              ");
            sql.AppendLine("    mid.included_columns,                ");
            sql.AppendLine("    migs.unique_compiles,                ");
            sql.AppendLine("    migs.user_seeks,                     ");
            sql.AppendLine("    migs.user_scans,                     ");
            sql.AppendLine("    migs.last_user_seek,                 ");
            sql.AppendLine("    migs.last_user_scan,                 ");
            sql.AppendLine("    migs.avg_total_user_cost,            ");
            sql.AppendLine("    migs.avg_user_impact                 ");
            sql.AppendLine("    FROM sys.dm_db_missing_index_group_stats migs ");
            sql.AppendLine("  INNER JOIN sys.dm_db_missing_index_groups mig   ");
            sql.AppendLine("    ON migs.group_handle = mig.index_group_handle ");
            sql.AppendLine("  INNER JOIN sys.dm_db_missing_index_details mid  ");
            sql.AppendLine("    ON mig.index_handle = mid.index_handle        ");
            sql.AppendLine("  WHERE mid.database_id = DB_ID()                 ");
            sql.AppendLine("  OPTION (RECOMPILE);                             ");
            ExecSql(sql.ToString());
        }

        public void TruncateQueryStatistics()
        {
            TruncateTable(DATABASE_NAME, TABLE_QUERIES);
        }

        public void TruncateQueryStatistics(string targetDatabase, string targetTable)
        {
            TruncateTable(targetDatabase, targetTable);
        }

        public void TruncateProcStatistics()
        {
            TruncateTable(DATABASE_NAME, TABLE_PROCS);
        }

        public void TruncateProcStatistics(string targetDatabase, string targetTable)
        {
            TruncateTable(targetDatabase, targetTable);
        }

        public void TruncateIndexStatistics()
        {
            TruncateTable(DATABASE_NAME, TABLE_INDEXES);
        }

        public void TruncateIndexStatistics(string targetDatabase, string targetTable)
        {
            TruncateTable(targetDatabase, targetTable);
        }

        public void TruncateIndexMissingStatistics()
        {
            TruncateTable(DATABASE_NAME, TABLE_INDEXESMISSING);
        }

        public void TruncateIndexMissingStatistics(string targetDatabase, string targetTable)
        {
            TruncateTable(targetDatabase, targetTable);
        }

        private void TruncateTable(string targetDatabase, string targetTable)
        {
            var sql = new StringBuilder();
            sql.AppendLine("IF EXISTS (SELECT * FROM " + targetDatabase + ".sys.objects WHERE object_id = OBJECT_ID(N'" + targetDatabase + ".[dbo]." + targetTable + "') AND type in (N'U')) ");
            sql.AppendLine("BEGIN ");
            sql.AppendLine("  TRUNCATE TABLE " + targetDatabase + ".[dbo]." + targetTable + " ");
            sql.AppendLine("END ");

            ExecSql(sql.ToString());
        }

        /// <summary>
        /// Returns true if the object is function, false otherwise.
        /// </summary>
        /// <param name="objectName"></param>
        /// <returns></returns>
        public bool IsFunction(string objectName)
        {
            return GetObjectType(objectName) == SqlEntityType.Function;
        }

        /// <summary>
        /// Returns type of the sql object.
        /// </summary>
        /// <param name="objectName"></param>
        /// <returns></returns>
        public SqlEntityType GetObjectType(string objectName)
        {
            var sql = new StringBuilder();
            sql.AppendLine("SELECT type");
            sql.AppendLine("FROM sys.objects");
            sql.AppendLine("WHERE object_id = OBJECT_ID(N'[dbo]." + objectName + "')");
            var code = ExecSql<string>(sql.ToString());
            if (code == null)
            {
                throw new ApplicationException($"Object {objectName} does not exist in the database {ConnectionProvider.GetDatabaseName()}.");
            }
            switch (code.Trim())
            {
                case "V":
                    return SqlEntityType.View;
                case "P":
                    return SqlEntityType.Procedure;
                case "FN":
                case "IF":
                case "TF":
                case "FS":
                case "FT":
                    return SqlEntityType.Function;
                default: throw new NotSupportedException("Type of the entity was not recognized as a supported type.");
            }
        }

        public string RetrieveProcedureCode(string name, bool alterProcedure = false)
        {
            //get stored proc           
            var code = GetCode(name);
            //prepare for change
            if (alterProcedure)
            {
                code = new Regex(@"CREATE\sPROCEDURE").Replace(code, "ALTER PROCEDURE");
            }
            return code;
        }

        public string RetrieveCode(string objectName)
        {
            return GetCode(objectName);
        }

        /// <summary>
        /// Retrieves function code and returns it as it was a stored procedure.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public string RetrieveFunctionCodeAsProc(string name)
        {
            //get function code
            var code = GetCode(name);
            //prepare for change
            code = new Regex(@"CREATE\sFUNCTION").Replace(code, "CREATE PROCEDURE");
            code = new Regex(@"(RETURNS[\s]*)(@[\s\S]*)(AS[\s]*BEGIN)").Replace(code, "$3 \n DECLARE $2"); //makes procedure from function
            code = new Regex(@"(RETURNS[\s\S]*)(AS[\s]*RETURN)").Replace(code, ""); 
            code = new Regex(@"(RETURNS[\s\S]*?)(AS[\s\S]*)").Replace(code, "$2");
            code = new Regex(@"(RETURN[\s\S]*@[\s\S]*)(END)").Replace(code, "$2");
            return code;
        }

        /// <summary>
        /// Retrieves view code and encapsulates the SELECT * FROM the view in a stored procedure.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public string RetrieveViewCodeAsProc(string name)
        {
            //get function code
            var code = GetCode(name);
            //prepare for change
            //prepare for change
            code = new Regex(@"CREATE\sVIEW").Replace(code, "CREATE PROCEDURE"); //makes procedure from view
            return code;
        }

        /// <summary>
        /// Drops procedure or function by name.
        /// </summary>
        public void DropProcedureOrFunction(string name)
        {
            ExecSql(string.Format("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{0}') AND type IN ( N'FN', N'IF', N'TF', N'FS', N'FT' )) DROP FUNCTION {0} ", name));
            ExecSql(string.Format("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{0}') AND type IN ( N'V', N'V ')) DROP VIEW {0} ", name));
            ExecSql(string.Format("IF (OBJECT_ID(N'{0}') IS NOT NULL) DROP PROCEDURE {0} ", name));
        }

        /// <summary>
        /// Retrieves parameters of the stored proc. The value gets cached for repeated use.
        /// </summary>
        public SqlParameterCollection GetProcParameters(string procedureName)
        {

            SqlParameterCollection parameters;
            //try to get value from cache
            if (_procParametersCache.TryGetValue(procedureName, out parameters))
            {
                return parameters;
            }
            SqlCommand cmd;
            if (_connection != null)
            {
                cmd = new SqlCommand(procedureName, _connection) { CommandType = CommandType.StoredProcedure };
                SqlCommandBuilder.DeriveParameters(cmd);
            }
            else
            {
                using (var connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID)))
                {
                    connection.Open();
                    cmd = new SqlCommand(procedureName, connection) { CommandType = CommandType.StoredProcedure };
                    SqlCommandBuilder.DeriveParameters(cmd);
                }
            }
            //cache the value
            _procParametersCache.Add(procedureName, cmd.Parameters);
            return cmd.Parameters;
        }

        /// <summary>
        /// Retrieves parameters of the stored procs and returns a comma separated list of parameter names and types. The format is intended for use in proc generation.
        /// </summary>
        public string GetProcParametersAsTSql(string procName)
        {
            var parameters = GetProcParameters(procName);

            var sb = new StringBuilder();
            foreach (SqlParameter p in parameters)
            {
                if (p.Direction == ParameterDirection.ReturnValue) { continue; }

                var type = p.SqlDbType.ToString().ToLower();
                sb.Append((sb.Length == 0 ? "" : ", ") + p.ParameterName + " " + type + (p.Scale > 0 ? $"({p.Precision},{p.Scale})" : "") + (type.EndsWith("char") ? (p.Size > 8000 ? "(max)" : $"({p.Size})") : "") + " = NULL ");
            }

            return sb.ToString();
        }

        public string GetProcDummyParameterValues(string procedureName)
        {
            var parameters = GetProcParameters(procedureName);

            var sb = new StringBuilder();
            foreach (SqlParameter p in parameters)
            {
                //add test for p type if possible - fine with zero so far
                if (p.Direction == ParameterDirection.ReturnValue)
                {
                    continue;
                }
                sb.Append(",0");
                //If you came here because you have failed test with (n)text consider changing it to (n)varchar(max)
            }
            var r = sb.ToString();
            return r.Length == 0 ? r : r.Substring(1); //omit first comma
        }

        private string GetCode(string name)
        {
            var ds = Fill("sp_helptext '" + name + "'");
            var sb = new StringBuilder();
            if (ds.Tables.Count == 0)
            {
                throw new ApplicationException($"Code of the object {name} cannot be extracted. It either does not exist or it is encrypted. Database: {ConnectionProvider.GetDatabaseName()}");
                //DEBUG: comment out the above exception and uncomment following code to allow encrypted or nonexisting procs;
                //Console.WriteLine(string.Format("Code of the stored procedure ({0}) cannot be extracted. It either does not exist or it is encrypted.", name));
                //return "";
            }
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                sb.Append(row["Text"]);
            }
            return sb.ToString();
        }

        public void RunSqlFile(string fullPath, bool consoleLog = false)
        {
            var content = File.ReadAllText(fullPath);
            var contents = Regex.Split(content + "\n", @"^\s*GO\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline);

            using (var connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID)))
            {
                connection.Open();
                var command = connection.CreateCommand();
                foreach (var cmd in contents)
                {
                    if (cmd.Trim() == "")
                        continue;

                    command.CommandText = cmd;
                    command.ExecuteNonQuery();
                }
            }
        }

        public Exception RunSqlFileEx(string fullPath, bool consoleLog = false)
        {
            try
            {
                RunSqlFile(fullPath, consoleLog);
            }
            catch (Exception ex)
            {
                return ex;
            }
            return null;
        }

        public void Execute(Action<SqlConnection> code)
        {
            _connection = new SqlConnection(ConnectionProvider.GetConnectionString(CONNECTION_ID));
            try
            {
                _connection.Open();
                code(_connection);
            }
            finally
            {
                _connection.Close();
                _connection.Dispose();
                _connection = null;
            }
        }

        public IList<StoredProc> GetProcedures()
        {
            //get all stored procedures from the database - not functions
            return PopulateStoredProcResult("SELECT object_id, name FROM sys.procedures ORDER BY name ASC");
        }

        public IList<StoredProc> GetReadOnlyProcedures()
        {
            //get all stored procedures from the database - not functions
            var ds = Fill("SELECT object_id, name FROM sys.procedures ORDER BY name ASC");
            var result = new List<StoredProc>();
            //add all names to the list
            foreach (DataRow row in ds.Tables[0].Rows)
            {
                var name = row["name"].ToString();
                //add the stored proc to the list if itl is readonly
                if (GetProcedureReadOnlyFlag(name) == ReadOnlyFlag.ReadOnly)
                {
                    result.Add(new StoredProc
                    {
                        Id = Convert.ToInt32(row["object_id"]),
                        Name = name,
                    });
                }
            }
            return result;
        }

        public IList<StoredProc> GetDependentProcedures(int procedureId)
        {
            return DoGetDependentProcedures(procedureId.ToString());
        }

        public IList<StoredProc> GetDependentProcedures(string procedureName)
        {
            return DoGetDependentProcedures($"OBJECT_ID('{procedureName}')");
        }

        public IList<StoredProc> GetDependentProceduresRec(int procedureId)
        {
            return DoGetDependentProceduresRec(procedureId.ToString());
        }

        public IList<StoredProc> GetDependentProceduresRec(string procedureName)
        {
            return DoGetDependentProceduresRec($"OBJECT_ID('{procedureName}')");
        }

        private IList<StoredProc> DoGetDependentProcedures(string procedureId)
        {
            //get all stored procedures depending on a procedure
            var query = $"SELECT sed.referenced_id [object_id], p.name FROM sys.sql_expression_dependencies sed INNER JOIN sys.procedures p ON p.object_id = sed.referenced_id WHERE sed.referencing_id = {procedureId}";
            return PopulateStoredProcResult(query);
        }

        private IList<StoredProc> DoGetDependentProceduresRec(string procedureId)
        {
            IList<StoredProc> result;
            if (_procedureDependenciesRec.TryGetValue(procedureId, out result)) { return result; }

            //get all stored procedures depending on a procedure
            var sb = new StringBuilder();
            sb.AppendLine("WITH funcs AS ( ");
            sb.AppendLine(
                "  SELECT sed.referenced_id, sed.referencing_id, 0 [level] FROM sys.sql_expression_dependencies sed ");
            sb.AppendLine("  INNER JOIN sys.procedures p ");
            sb.AppendLine("    ON p.object_id = sed.referenced_id ");
            sb.AppendLine($" WHERE referencing_id = {procedureId}  ");
            sb.AppendLine("  AND sed.referenced_id <> referencing_id ");
            sb.AppendLine("  UNION ALL ");
            sb.AppendLine("  SELECT sed.referenced_id, sed.referencing_id, [level]+1 ");
            sb.AppendLine("  FROM sys.sql_expression_dependencies sed ");
            sb.AppendLine("  INNER JOIN sys.procedures p ");
            sb.AppendLine("    ON p.object_id = sed.referenced_id ");
            sb.AppendLine("  INNER JOIN funcs f ");
            sb.AppendLine("    ON f.referenced_id = sed.referencing_id ");
            sb.AppendLine("  WHERE sed.referenced_id <> f.referencing_id ");
            sb.AppendLine(") ");
            sb.AppendLine("SELECT DISTINCT");
            sb.AppendLine("  referenced_id [object_id], ");
            sb.AppendLine("  OBJECT_NAME(referenced_id) [name] ");
            sb.AppendLine("FROM funcs  ");

            result = PopulateStoredProcResult(sb.ToString());
            _procedureDependenciesRec[procedureId] = result;
            return result;
        }

        private IList<StoredProc> PopulateStoredProcResult(string query)
        {
            var result = new List<StoredProc>();
            var ds = Fill(query);
            //add ids of all dependent procedures to the output list
            ds.Tables[0].AsEnumerable().ForEach(r => result.Add(new StoredProc
            {
                Id = Convert.ToInt32(r["object_id"]),
                Name = r["name"].ToString()
            }));
            return result;
        }

        public ReadOnlyFlag GetProcedureReadOnlyFlag(string procedureName)
        {
            var result = ReadOnlyFlag.None;
            if (_procedureFlags.TryGetValue(procedureName, out result)) { return result; }

            if (IsFunction(procedureName))
            {
                result = ReadOnlyFlag.ReadOnly;
                _procedureFlags[procedureName] = result;
                return result;
            }

            var code = GetCode(procedureName);
            if (Regex.IsMatch(code, @"CREATE([\s]*)\/\*READONLY\*\/([\s]*)PROCEDURE",
                RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Compiled))
            {
                result = ReadOnlyFlag.ReadOnly;
            }
            else if (Regex.IsMatch(code, @"CREATE([\s]*)\/\*READWRITE\*\/([\s]*)PROCEDURE",
                RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Compiled))
            {
                result = ReadOnlyFlag.ReadWrite;
            }
            _procedureFlags[procedureName] = result;
            return result;
        }

    }

    public class StoredProc
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }

    public enum ReadOnlyFlag
    {
        None,
        ReadOnly,
        ReadWrite,
    }

}
