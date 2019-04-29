using System;
using System.ComponentModel;
using System.Data;
using NBO.Test.Framework;

namespace SQLPerf
{
    public class SQLPerfExecutor
    {
        private const string SqlPerfStatsTableName = "__SQLPerfStats";
        private SqlExecutor _sqlExecutor;
        private BackgroundWorker _worker;

        public delegate void DisplayOutputTextEventHandler(string outputTextToAdd, bool clearPreviousText);
        public event DisplayOutputTextEventHandler DisplayOutputTextEvent;


        public delegate void WorkerCompleted();
        public event WorkerCompleted OnWorkerCompleted;

        public SQLPerfExecutor()
        {
            InitializeBackgroundWorker();
        }

        private void InitializeBackgroundWorker()
        {
            _worker = new BackgroundWorker();

            _worker.DoWork += new DoWorkEventHandler(backgroundWorker_DoWork);
            _worker.RunWorkerCompleted += new RunWorkerCompletedEventHandler(backgroundWorker1_RunWorkerCompleted);

            _worker.WorkerReportsProgress = true;
        }

        private void backgroundWorker1_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            OnWorkerCompleted?.Invoke();
        }

        private void backgroundWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            var workerParam = (WorkerParams)e.Argument;

            try
            {
                for (var i = 1; i <= workerParam.NumberOfIterations; i++)
                {
                    _sqlExecutor.FreeProcCache();
                    var startedRunTime = DateTime.Now;
                    _sqlExecutor.ExecSql(workerParam.SqlToExecute);
                    OutputText($"backgroundWorker executed run {i} of {workerParam.NumberOfIterations}.  Started: {startedRunTime} and Completed: {DateTime.Now}", false);
                    _sqlExecutor.CollectProcStatistics(i == 1, $"RUN #{i}", _sqlExecutor.ConnectionProvider.GetInitialCatalog(), SqlPerfStatsTableName);
                }

                var perfStats = _sqlExecutor.Fill($"SELECT * FROM dbo.{SqlPerfStatsTableName}").Tables[0];
                CreateAverageRow(perfStats);

                _sqlExecutor.WriteCsvFile(perfStats, "SQLPerf.csv", ",");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }
        }

        // This all needs work...
        private void CreateAverageRow(DataTable perfStats)
        {
            var numberOfRowsToAverage = perfStats.Rows.Count;

            var dr = perfStats.NewRow();
            foreach (DataColumn currentCol in perfStats.Columns)
            {
                var colDataType = currentCol.DataType;
                if (colDataType == typeof(string))
                {
                    continue;
                }
                else if (colDataType == typeof(long))
                {
                    long averageValue = 0;
                    foreach (DataRow currentRow in perfStats.Rows)
                    {
                        averageValue += (long)currentRow[currentCol];
                    }

                    averageValue = averageValue / numberOfRowsToAverage;
                    dr[currentCol] = averageValue;
                }
                else if (colDataType == typeof(int))
                {
                    int averageValue = 0;
                    foreach (DataRow currentRow in perfStats.Rows)
                    {
                        averageValue += (int)currentRow[currentCol];
                    }

                    averageValue = averageValue / numberOfRowsToAverage;
                    dr[currentCol] = averageValue;
                }
                else if (colDataType == typeof(double))
                {
                    double averageValue = 0;
                    foreach (DataRow currentRow in perfStats.Rows)
                    {
                        averageValue += (double)currentRow[currentCol];
                    }

                    averageValue = averageValue / numberOfRowsToAverage;
                    dr[currentCol] = averageValue;
                }
            }

            dr["test"] = "AVERAGE";
            perfStats.Rows.Add(dr);
            perfStats.AcceptChanges();
        }

        public void Run(string connectionString, string sqlToExecute, int numberOfIterations)
        {
            var sqlTestConnectionProvider = new SqlConnectionProvider(connectionString);
            _sqlExecutor = new SqlExecutor(sqlTestConnectionProvider);

            var workerParams = new WorkerParams(sqlToExecute, numberOfIterations);

            _worker.RunWorkerAsync(workerParams);
        }

        private void OutputText(string text, bool clearPreviousText)
        {
            DisplayOutputTextEvent?.Invoke(text, clearPreviousText);
        }

        private class WorkerParams
        {
            public readonly string SqlToExecute;
            public readonly int NumberOfIterations;

            public WorkerParams(string sqlToExecuteParam, int numberOfIterationsParam)
            {
                SqlToExecute = sqlToExecuteParam;
                NumberOfIterations = numberOfIterationsParam;
            }
        }
    }
}
