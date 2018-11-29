using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.Dynamic;
using DevExpress.XtraEditors;
using System.IO;

namespace DataDeliver
{
    public partial class MainFrame : Form
    {
        public Action<bool, int> ReportMysqlProgress;
        public Action<bool, int> ReportMongoProgress;
        public Action<int> ReportKbaseProgress;
        public MainFrame()
        {
            InitializeComponent();
            ReportMysqlProgress = ReportMysqlProgressBarStatus;
            ReportMongoProgress = UpdateProgressBarStatus;
            ReportKbaseProgress = UpdateKbaseProgressBarStatus;
        }

        /// <summary>
        /// 页面首次加载，只运行一次
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void xtraTabControl1_Enter(object sender, EventArgs e)
        {
            var TabName = xtraTabControl1.SelectedTabPage.Name;
            WindowInit(TabName);
        }

        /// <summary>
        /// 切换页面
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void xtraTabControl1_Click(object sender, EventArgs e)
        {
            var TabName = xtraTabControl1.SelectedTabPage.Name;
            WindowInit(TabName);
        }

        /// <summary>
        /// 窗口数据初始化
        /// </summary>
        /// <param name="tabName"></param>
        private void WindowInit(string tabName)
        {
            switch (tabName)
            {
                // 导出Mysql数据
                case "xtpExportMysql":

                    var MDbLists = MySqlHelper.GetDBNames();
                    var Mcols = cmbMysqlDbName.Properties.Items;
                    Mcols.BeginUpdate();
                    try
                    {
                        Mcols.Clear();
                        Mcols.AddRange(MDbLists);
                    }
                    catch (Exception)
                    {
                        Mcols.EndUpdate();
                    }

                    cmbMysqlDbName.SelectedIndex = -1;

                    if (string.IsNullOrEmpty(btnMysqlPath.Text))
                        btnMysqlPath.Text = AppDomain.CurrentDomain.BaseDirectory;

                    progressBarControl2.Properties.Step = 1;
                    progressBarControl2.Properties.PercentView = true;
                    progressBarControl2.Properties.Minimum = 0;

                    break;

                // 导出Kbase数据
                case "xtpExportKbase":

                    txbTable.Text = string.Empty;
                    txbWhere.Text = string.Empty;
                    gridKbase.DataSource = null;

                    if (string.IsNullOrEmpty(btnSavePath.Text))
                        btnSavePath.Text = AppDomain.CurrentDomain.BaseDirectory;

                    txbReportKbaseNum.Text = string.Empty;
                    break;

                // 导出Mongo数据
                case "tbpExport":

                    var DbLists = MongoHelper.GetDBNames();
                    var cols = cmbDbNames.Properties.Items;
                    cols.BeginUpdate();
                    try
                    {
                        cols.Clear();
                        cols.AddRange(DbLists);
                    }
                    catch (Exception)
                    {
                        cols.EndUpdate();
                    }

                    cmbDbNames.SelectedIndex = -1;

                    if (string.IsNullOrEmpty(btnFilePath.Text))
                        btnFilePath.Text = AppDomain.CurrentDomain.BaseDirectory;

                    progressBarControl1.Properties.Step = 1;
                    progressBarControl1.Properties.PercentView = true;
                    progressBarControl1.Properties.Minimum = 0;
                    break;
                // 上传更新数据
                case "tbpUpdate":

                    break;
                default:
                    break;
            }
        }

        /// <summary>
        /// Mysql导出数据，数据库发生变化
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void cmbMysqlDbName_TextChanged(object sender, EventArgs e)
        {
            List<string> Colletions = new List<string>();
            var DbName = cmbMysqlDbName.Text;
            if (!string.IsNullOrEmpty(DbName))
            {
                Colletions = MySqlHelper.GetTables(DbName);
            }

            var cols = cmbMysqlTableName.Properties.Items;
            cols.BeginUpdate();
            try
            {
                cols.Clear();
                cols.AddRange(Colletions);
            }
            catch (Exception)
            {
                cols.EndUpdate();
            }

            cmbMysqlTableName.SelectedIndex = -1;
        }

        /// <summary>
        /// Mysql导出数据，表名发生变化
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void cmbMysqlTableName_TextChanged(object sender, EventArgs e)
        {
            var DbName = cmbMysqlDbName.Text;
            var Clt = cmbMysqlTableName.Text;

            if (string.IsNullOrEmpty(DbName) || string.IsNullOrEmpty(Clt))
            {
                grdMysql.DataSource = null;
                return;
            }

            // 方式一
            List<dynamic> datas = new List<dynamic>();
            // 方式二
            //List<ColumnInfos> datas = new List<ColumnInfos>();

            var Cols = MySqlHelper.GetColumns(DbName, Clt);

            if (Cols == null || Cols.Count == 0)
                return;

            Cols.ForEach(d =>
            {
                // 方式一
                dynamic data = new ExpandoObject();
                data.MysqlField = d;
                data.ExportField = string.Empty;
                datas.Add(data);

                // 方式二
                //datas.Add(new ColumnInfos
                //{
                //    MongoField = d,
                //    KbaseField = string.Empty
                //});

            });
            repositoryItemComboBox3.Items.AddRange(new List<string> { "文件名", "正文", "题名", "关键词", "摘要", "主题" });

            grdMysql.DataSource = datas;

        }

        /// <summary>
        /// Mysql导出数据，开始导出
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnExportMysql_Click(object sender, EventArgs e)
        {
            Dictionary<string, string> ExportDics = new Dictionary<string, string>();

            var sa = gridView4.GetSelectedRows();
            foreach (var item in sa)
            {
                dynamic d = gridView4.GetRow(item) as dynamic;
                ExportDics.Add(d.MysqlField, d.ExportField);
            }

            if (ExportDics.Count == 0)
            {
                XtraMessageBox.Show("请选择导出的字段...", "提示");
                return;
            }
            var tt = Task.Run(() =>
            {
                return MySqlHelper.ExportData(cmbMysqlDbName.Text, cmbMysqlTableName.Text, txbMysqlWhere.Text, ExportDics, btnMysqlPath.Text, ReportMysqlProgress);
            });

            tt.ContinueWith(t =>
            {
                this.Invoke((MethodInvoker)delegate
                {
                    XtraMessageBox.Show(t.Result.Item2, t.Result.Item1 ? "导出成功" : "导出失败");
                });
            });
        }

        /// <summary>
        /// Mysql导出数据，报告进度（Mysql）
        /// </summary>
        /// <param name="isInit"></param>
        /// <param name="maxNum"></param>
        public void ReportMysqlProgressBarStatus(bool isInit, int maxNum = 0)
        {
            this.Invoke((MethodInvoker)delegate
            {
                if (isInit)
                {
                    progressBarControl2.Properties.Maximum = maxNum;
                }
                else
                {
                    progressBarControl2.PerformStep();
                    progressBarControl2.Update();
                }
            });
        }
        /// <summary>
        /// Mongo导出数据，Mongo DB 发生变化
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void cmbDbNames_TextChanged(object sender, EventArgs e)
        {
            List<string> Colletions = new List<string>();
            var DbName = cmbDbNames.Text;
            if (!string.IsNullOrEmpty(DbName))
            {
                Colletions = MongoHelper.GetCollections(DbName);
            }

            var cols = cmbCollections.Properties.Items;
            cols.BeginUpdate();
            try
            {
                cols.Clear();
                cols.AddRange(Colletions);
            }
            catch (Exception)
            {
                cols.EndUpdate();
            }

            cmbCollections.SelectedIndex = -1;
        }

        /// <summary>
        /// Mongo导出数据，Mongo Collection 发生变化
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void cmbCollections_TextChanged(object sender, EventArgs e)
        {
            var DbName = cmbDbNames.Text;
            var Clt = cmbCollections.Text;

            if (string.IsNullOrEmpty(DbName) || string.IsNullOrEmpty(Clt))
                return;

            // 方式一
            List<dynamic> datas = new List<dynamic>();
            // 方式二
            //List<ColumnInfos> datas = new List<ColumnInfos>();

            var Cols = MongoHelper.GetColumns(DbName, Clt);

            if (Cols == null || Cols.Count == 0)
                return;

            Cols.ToList().ForEach(d =>
            {
                // 方式一
                dynamic data = new ExpandoObject();
                data.MongoField = d;
                data.KbaseField = string.Empty;
                datas.Add(data);

                // 方式二
                //datas.Add(new ColumnInfos
                //{
                //    MongoField = d,
                //    KbaseField = string.Empty
                //});

            });
            repositoryItemComboBox1.Items.AddRange(new List<string> { "文件名", "正文", "题名", "关键词", "摘要", "主题" });

            grdFields.DataSource = datas;
        }

        /// <summary>
        /// Mongo导出数据，开始导出
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnStart_Click(object sender, EventArgs e)
        {
            Dictionary<string, string> ExportDics = new Dictionary<string, string>();
            if (string.IsNullOrEmpty(cmbDbNames.Text))
            {
                XtraMessageBox.Show("请选择导出的数据库来源...", "提示");
                return;
            }

            if (string.IsNullOrEmpty(cmbCollections.Text))
            {
                XtraMessageBox.Show("请选择导出的数据集合...", "提示");
                return;
            }

            var sa = gridView1.GetSelectedRows();
            foreach (var item in sa)
            {
                dynamic d = gridView1.GetRow(item) as dynamic;
                ExportDics.Add(d.MongoField, d.KbaseField);
            }

            if (ExportDics.Count == 0)
            {
                XtraMessageBox.Show("请选择导出的字段...", "提示");
                return;
            }
            var tt = Task.Run(() =>
            {
                return MongoHelper.ExportData(cmbDbNames.Text, cmbCollections.Text, ExportDics, btnFilePath.Text, ReportMongoProgress);
            });

            tt.ContinueWith(t =>
            {
                this.Invoke((MethodInvoker)delegate
                {
                    XtraMessageBox.Show(t.Result.Item2, t.Result.Item1 ? "导出成功" : "导出失败");
                });
            });
        }

        /// <summary>
        /// Mongo导出数据，报告进度（Mongo）
        /// </summary>
        /// <param name="isInit"></param>
        /// <param name="maxNum"></param>
        public void UpdateProgressBarStatus(bool isInit, int maxNum = 0)
        {
            this.Invoke((MethodInvoker)delegate
            {
                if (isInit)
                {
                    progressBarControl1.Properties.Maximum = maxNum;
                }
                else
                {
                    progressBarControl1.PerformStep();
                    progressBarControl1.Update();
                }
            });
        }


        /// <summary>
        /// Kbase导出数据，Kbase表名发生变化时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void txbTable_EditValueChanged(object sender, EventArgs e)
        {
            var tableName = txbTable.Text;
            var Cols = KbaseHelper.GetCols(tableName);

            if (Cols == null || Cols.Count == 0)
                return;
            List<dynamic> Datas = new List<dynamic>();

            foreach (var item in Cols)
            {
                dynamic data = new ExpandoObject();
                data.KbaseB = item;
                data.KbaseE = string.Empty;
                Datas.Add(data);
            }
            repositoryItemComboBox2.Items.AddRange(new List<string> { "文件名", "正文", "题名", "关键词", "摘要", "主题" });
            gridKbase.DataSource = Datas;

        }

        /// <summary>
        /// Kbase数据导出
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnExportKbase_Click(object sender, EventArgs e)
        {
            Dictionary<string, string> ExportDics = new Dictionary<string, string>();
            if (string.IsNullOrEmpty(txbTable.Text))
            {
                XtraMessageBox.Show("请输入Kbase表名...", "提示");
                return;
            }


            var sa = gridView2.GetSelectedRows();
            foreach (var item in sa)
            {
                dynamic d = gridView2.GetRow(item) as dynamic;
                ExportDics.Add(d.KbaseB, d.KbaseE);
            }

            if (ExportDics.Count == 0)
            {
                XtraMessageBox.Show("请选择导出的字段...", "提示");
                return;
            }
            var tt = Task.Run(() =>
            {
                return KbaseHelper.ExportData(txbTable.Text, txbWhere.Text, ExportDics, btnSavePath.Text, ReportKbaseProgress);
            });

            tt.ContinueWith(t =>
            {
                this.Invoke((MethodInvoker)delegate
                {
                    XtraMessageBox.Show(t.Result.Item2, t.Result.Item1 ? "导出成功" : "导出失败");
                });
            });
        }

        /// <summary>
        /// Kbase导出数据，报告进度（Kbase）
        /// </summary>
        /// <param name="isInit"></param>
        /// <param name="maxNum"></param>
        public void UpdateKbaseProgressBarStatus(int maxNum = 0)
        {
            this.Invoke((MethodInvoker)delegate
            {
                txbReportKbaseNum.Text = maxNum.ToString();
            });
        }

        /// <summary>
        /// 上传更新文件
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void btnUpload_Click(object sender, EventArgs e)
        {
            if (string.IsNullOrEmpty(labOK.Text))
            {
                XtraMessageBox.Show("Kbase表名无效！");
                return;
            }

            var FileName = btnLoadFiles.Text;
            if (Path.GetExtension(FileName).ToLower() != ".txt")
            {
                XtraMessageBox.Show("仅支持REC文件！");
                return;
            }


            btnUpload.Enabled = false;
            var t = Task.Run(() =>
            {
                try
                {
                    var BaseDir = @"\\192.168.106.60\RecFiles\机标关键词";
                    Directory.CreateDirectory(BaseDir);
                    var BaseFileName = Path.GetFileName(FileName);
                    this.Invoke((MethodInvoker)delegate
                    {
                        txbProcessor.Text += "正在拷贝文件..." + Environment.NewLine;
                    });
                    File.Copy(FileName, Path.Combine(BaseDir, BaseFileName), true);
                    this.Invoke((MethodInvoker)delegate
                    {
                        txbProcessor.Text += "正在上传数据..." + Environment.NewLine;
                    });

                    var ur = KbaseHelper.UploadData(txbKbasename.Text, BaseFileName);
                    this.Invoke((MethodInvoker)delegate
                    {
                        txbProcessor.Text += ur.Item2 + Environment.NewLine;
                        txbProcessor.Text += $"完成！上传{(ur.Item1 ? "成功" : "失败")}!";
                    });
                }
                catch (Exception ee)
                {
                    this.Invoke((MethodInvoker)delegate
                    {
                        txbProcessor.Text += ee.Message;
                    });
                }
            });

            t.ContinueWith(st =>
            {
                this.Invoke((MethodInvoker)delegate
                {
                    btnUpload.Enabled = true;
                });
            });

        }

        /// <summary>
        /// 同步上传时，KbaseName 发生变化时
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void txbKbasename_EditValueChanged(object sender, EventArgs e)
        {
            var tableName = txbKbasename.Text;

            if (KbaseHelper.GetCols(tableName).Count == 0)
                labOK.Text = string.Empty;
            else
                labOK.Text = "√";
        }

        private void MainFrame_FormClosing(object sender, FormClosingEventArgs e)
        {
            KbaseHelper.Dispose();
        }
    }

    /// <summary>
    /// Mongo导出字段配置类
    /// </summary>
    public class ColumnInfos
    {
        /// <summary>
        /// Mongo原字段
        /// </summary>
        public string MongoField { get; set; }
        /// <summary>
        /// 导出后字段（重命名）
        /// </summary>
        public string KbaseField { get; set; }
    }
}
