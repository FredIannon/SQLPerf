using System;
using System.Text;
using System.Windows.Forms;

namespace SQLPerf
{
    public partial class SQLPerf : Form
    {
        readonly SQLPerfExecutor _sqlPerfExecutor;
        public delegate void UpdateOutputTextDelegate(string outputTextToAdd, bool clearPreviousText);

        public SQLPerf()
        {
            InitializeComponent();

            _sqlPerfExecutor = new SQLPerfExecutor();
            _sqlPerfExecutor.OnWorkerCompleted += SqlPerfExecutorOnOnWorkerCompleted;
            _sqlPerfExecutor.DisplayOutputTextEvent += SqlPerfExecutorOnDisplayOutputTextEvent;

            SetupDefaults();
        }

        private void SetupDefaults()
        {
            var sql = new StringBuilder();
            sql.AppendLine("CREATE TABLE #ItemCost (ItemNumber int primary key, StandardUnitCost float )");
            sql.AppendLine("");
            sql.AppendLine("INSERT INTO #ItemCost VALUES (1013706, null)");
            sql.AppendLine("INSERT INTO #ItemCost VALUES (1008754, null)");
            sql.AppendLine("");
            sql.AppendLine("DECLARE	@return_value int");
            sql.AppendLine("EXEC	@return_value = [dbo].[Items_UpdateItemCostTemp]");
            sql.AppendLine("        @RecipeSiteNumber = 5,");
            sql.AppendLine("        @CostSiteNumber = 5,");
            sql.AppendLine("        @CostDate = N'4/1/2019 12:00:00 am'");
            sql.AppendLine("");
            sql.AppendLine("SELECT * FROM #ItemCost");
            sql.AppendLine("");
            sql.AppendLine("DROP TABLE #ItemCost");
            sql.AppendLine("");
            TSQLToExecute.Text = sql.ToString();

            connectionString.Text = @"Data Source=dfwfiannon2.corp.ncr.com\MSSQL2016;Initial Catalog=ML_SodexoDrive_PRD_20190225;Persist Security Info=True;User ID=sa;Password=NcrNcr123";
        }

        private void RunIt_Click(object sender, EventArgs e)
        {
            SqlPerfExecutorOnDisplayOutputTextEvent($"Starting at: {DateTime.Now}", true);
            btnRunIt.Enabled = false;

            var numberOfIterationsToExecute = Convert.ToInt32(numberOfIterations.Text);

            try
            {
                Cursor.Current = Cursors.WaitCursor;
                _sqlPerfExecutor.Run(connectionString.Text, TSQLToExecute.Text, numberOfIterationsToExecute);
            }
            catch (Exception ex)
            {
                OutputTxtBox.Text = ex.ToString();
                SqlPerfExecutorOnOnWorkerCompleted();
            }
        }

        private void SqlPerfExecutorOnOnWorkerCompleted()
        {
            btnRunIt.Enabled = true;
            Cursor.Current = Cursors.Default;
            SqlPerfExecutorOnDisplayOutputTextEvent($"Completed at: {DateTime.Now}", false);
        }

        private void SqlPerfExecutorOnDisplayOutputTextEvent(string outputTextToAdd, bool clearPreviousText)
        {
            try
            {
                if (InvokeRequired)
                {
                    Invoke(new UpdateOutputTextDelegate(UpdateOutputText), outputTextToAdd, clearPreviousText);
                }
                else
                {
                    UpdateOutputText(outputTextToAdd, clearPreviousText);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        private void UpdateOutputText(string outputTextToAdd, bool clearPreviousText)
        {
            if (clearPreviousText)
            {
                OutputTxtBox.Text = string.Empty;
            }

            OutputTxtBox.Text += outputTextToAdd + Environment.NewLine;
        }
    }
}
