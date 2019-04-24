namespace SQLPerf
{
    partial class SQLPerf
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.btnRunIt = new System.Windows.Forms.Button();
            this.TSQLToExecute = new System.Windows.Forms.TextBox();
            this.OutputTxtBox = new System.Windows.Forms.TextBox();
            this.label1 = new System.Windows.Forms.Label();
            this.numberOfIterations = new System.Windows.Forms.TextBox();
            this.label2 = new System.Windows.Forms.Label();
            this.connectionString = new System.Windows.Forms.TextBox();
            this.SuspendLayout();
            // 
            // btnRunIt
            // 
            this.btnRunIt.Location = new System.Drawing.Point(360, 415);
            this.btnRunIt.Name = "btnRunIt";
            this.btnRunIt.Size = new System.Drawing.Size(75, 23);
            this.btnRunIt.TabIndex = 6;
            this.btnRunIt.Text = "Run it baby";
            this.btnRunIt.UseVisualStyleBackColor = true;
            this.btnRunIt.Click += new System.EventHandler(this.RunIt_Click);
            // 
            // TSQLToExecute
            // 
            this.TSQLToExecute.Location = new System.Drawing.Point(13, 13);
            this.TSQLToExecute.Multiline = true;
            this.TSQLToExecute.Name = "TSQLToExecute";
            this.TSQLToExecute.ScrollBars = System.Windows.Forms.ScrollBars.Both;
            this.TSQLToExecute.Size = new System.Drawing.Size(775, 241);
            this.TSQLToExecute.TabIndex = 0;
            // 
            // OutputTxtBox
            // 
            this.OutputTxtBox.Location = new System.Drawing.Point(13, 305);
            this.OutputTxtBox.Multiline = true;
            this.OutputTxtBox.Name = "OutputTxtBox";
            this.OutputTxtBox.ReadOnly = true;
            this.OutputTxtBox.ScrollBars = System.Windows.Forms.ScrollBars.Both;
            this.OutputTxtBox.Size = new System.Drawing.Size(775, 104);
            this.OutputTxtBox.TabIndex = 3;
            // 
            // label1
            // 
            this.label1.AutoSize = true;
            this.label1.Location = new System.Drawing.Point(86, 425);
            this.label1.Name = "label1";
            this.label1.Size = new System.Drawing.Size(104, 13);
            this.label1.TabIndex = 4;
            this.label1.Text = "Number of iterations:";
            // 
            // numberOfIterations
            // 
            this.numberOfIterations.Location = new System.Drawing.Point(197, 425);
            this.numberOfIterations.Name = "numberOfIterations";
            this.numberOfIterations.Size = new System.Drawing.Size(34, 20);
            this.numberOfIterations.TabIndex = 5;
            this.numberOfIterations.Text = "5";
            // 
            // label2
            // 
            this.label2.AutoSize = true;
            this.label2.Location = new System.Drawing.Point(13, 272);
            this.label2.Name = "label2";
            this.label2.Size = new System.Drawing.Size(92, 13);
            this.label2.TabIndex = 1;
            this.label2.Text = "Connection string:";
            // 
            // connectionString
            // 
            this.connectionString.Location = new System.Drawing.Point(112, 272);
            this.connectionString.Name = "connectionString";
            this.connectionString.ScrollBars = System.Windows.Forms.ScrollBars.Horizontal;
            this.connectionString.Size = new System.Drawing.Size(676, 20);
            this.connectionString.TabIndex = 2;
            // 
            // SQLPerf
            // 
            this.AcceptButton = this.btnRunIt;
            this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
            this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
            this.ClientSize = new System.Drawing.Size(800, 450);
            this.Controls.Add(this.connectionString);
            this.Controls.Add(this.label2);
            this.Controls.Add(this.numberOfIterations);
            this.Controls.Add(this.label1);
            this.Controls.Add(this.OutputTxtBox);
            this.Controls.Add(this.TSQLToExecute);
            this.Controls.Add(this.btnRunIt);
            this.Name = "SQLPerf";
            this.Text = "SQLPerf";
            this.ResumeLayout(false);
            this.PerformLayout();

        }

        #endregion

        private System.Windows.Forms.Button btnRunIt;
        private System.Windows.Forms.TextBox TSQLToExecute;
        private System.Windows.Forms.TextBox OutputTxtBox;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.TextBox numberOfIterations;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.TextBox connectionString;
    }
}

