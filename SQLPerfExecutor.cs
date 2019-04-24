using System;
using System.ComponentModel;
using System.Data;
using System.Threading;
using NBO.Test.Framework;

namespace SQLPerf
{
    public class SQLPerfExecutor
    {
        private string SQLPerfStatsTableName = "__SQLPerfStats";
        private SqlExecutor _sqlExecutor;

        public delegate void DisplayOutputTextEventHandler(string outputTextToAdd, bool clearPreviousText);
        public event DisplayOutputTextEventHandler DisplayOutputTextEvent;

        private BackgroundWorker _worker;

        public delegate void WorkerCompleted();
        public event WorkerCompleted OnWorkerCompleted;

        public SQLPerfExecutor()
        {
            InitializeBackgroundWorker();
        }

        private void InitializeBackgroundWorker()
        {
            _worker = new BackgroundWorker();
//            _worker.WorkerSupportsCancellation = true;

            _worker.DoWork += new DoWorkEventHandler(backgroundWorker_DoWork);
            _worker.RunWorkerCompleted += new RunWorkerCompletedEventHandler(backgroundWorker1_RunWorkerCompleted);

            _worker.WorkerReportsProgress = true;
//            _worker.ProgressChanged += new ProgressChangedEventHandler(backgroundWorker1_ProgressChanged);
        }

/*
        private void backgroundWorker1_ProgressChanged(object sender, ProgressChangedEventArgs e)
        {
            OutputText($"backgroundWorker is starting: {DateTime.Now}", false);
        }
*/

        private void backgroundWorker1_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            OnWorkerCompleted?.Invoke();
        }

        private void backgroundWorker_DoWork(object sender, DoWorkEventArgs e)
        {
            var workerParam = (WorkerParams)e.Argument;

            try
            {
                for (var i = 1; i <= workerParam.numberOfIteration; i++)
                {
                    OutputText($"backgroundWorker is starting run {i} of {workerParam.numberOfIteration} at {DateTime.Now}", false);

                    _sqlExecutor.FreeProcCache();
                    _sqlExecutor.ExecSql(workerParam.tSQLToExecute);
                    _sqlExecutor.CollectProcStatistics(i == 1, $"SQLPerf #{i}", _sqlExecutor.ConnectionProvider.GetInitialCatalog(), SQLPerfStatsTableName);

                    Thread.Sleep(1000 * 1); // for some reason I sometimes lose rows in CollectProcStatistics
                    OutputText($"backgroundWorker has completed run {i} of {workerParam.numberOfIteration} at {DateTime.Now}", false);
                }

                var perfStats = _sqlExecutor.Fill($"SELECT * FROM dbo.{SQLPerfStatsTableName}").Tables[0];
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

            // blank row for prettiness
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

            var blankRow = perfStats.NewRow();
            perfStats.Rows.Add(blankRow);
            perfStats.AcceptChanges();

            perfStats.Rows.Add(dr);
            perfStats.AcceptChanges();
        }

        public void Run(string connectionString, string tSQLToExecute, int numberOfIterations)
        {
            var sqlTestConnectionProvider = new SqlConnectionProvider(connectionString);
            _sqlExecutor = new SqlExecutor(sqlTestConnectionProvider);


            var workerParams = new WorkerParams(tSQLToExecute, numberOfIterations);

            _worker.RunWorkerAsync(workerParams);

//            _sqlExecutor.ExecSql(tSQLToExecute);
        }

        private void OutputText(string text, bool clearPreviousText)
        {
            DisplayOutputTextEvent?.Invoke(text, clearPreviousText);
        }

        private class WorkerParams
        {
            public string tSQLToExecute;
            public int numberOfIteration;

            public WorkerParams(string tSqlToExecute, int numberOfIteration)
            {
                tSQLToExecute = tSqlToExecute;
                this.numberOfIteration = numberOfIteration;
            }
        }
    }
}
