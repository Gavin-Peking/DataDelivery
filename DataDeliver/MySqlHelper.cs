using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Dapper;
using System.IO;

namespace DataDeliver
{
    public class MySqlHelper
    {
        public static string _ConnStr = @"Server=192.168.106.60;Database={0};Uid=root;Pwd=cnkittod;default command timeout=7200;";


        internal static List<string> GetDBNames()
        {
            using (var Conn = new MySqlConnection(string.Format(_ConnStr, "wenku_data")))
            {
                return Conn.Query<string>("SHOW DATABASES;").ToList();
            }
        }


        internal static List<string> GetTables(string dbName)
        {
            using (var Conn = new MySqlConnection(string.Format(_ConnStr, dbName)))
            {
                return Conn.Query<string>("SHOW TABLES;").ToList();
            }
        }

        internal static List<string> GetColumns(string dbName, string tableName)
        {
            List<string> Cols = new List<string>();
            using (var Cursor = MySql.Data.MySqlClient.MySqlHelper.ExecuteReader(string.Format(_ConnStr, dbName), $"SELECT * FROM `{tableName}` limit 1;"))
            {
                for (int i = 0; i < Cursor.FieldCount; i++)
                {
                    Cols.Add(Cursor.GetName(i));
                }
            }
            return Cols;
        }

        internal static (bool, string) ExportData(string dbName, string tableName, string where, Dictionary<string, string> fields, string baseDir, Action<bool, int> report)
        {
            if (!string.IsNullOrEmpty(where))
            {
                if (!where.ToLower().Contains("where"))
                    where = " WHERE " + where;
            }
            try
            {
                using (var Conn = new MySqlConnection(string.Format(_ConnStr, dbName)))
                {
                    Conn.Open();
                    using (var cmd = new MySqlCommand())
                    {
                        cmd.Connection = Conn;
                        cmd.CommandText = string.Format("set net_write_timeout={0}; set net_read_timeout={1};", int.MaxValue, int.MaxValue);
                        cmd.ExecuteNonQuery();
                    }

                    var Sql = $"SELECT COUNT(*) FROM `{tableName}` {where} ;";
                    var DataCount = Conn.ExecuteScalar<int>(Sql);
                    if (DataCount > 0)
                    {
                        var FileFullPath = Path.Combine(baseDir, $"{dbName}_{tableName}_{DateTime.Now.ToString("yyyyMMddHHmmss")}.txt");
                        report?.Invoke(true, (int)DataCount);
                        using (var Writer = new StreamWriter(FileFullPath, false, Encoding.Default))
                        {
                            Sql = $@"SELECT {fields.Keys.Aggregate((c1, c2) => c1 + "," + c2)} FROM `{tableName}` {where};";
                            using (var Reader = MySql.Data.MySqlClient.MySqlHelper.ExecuteReader(Conn, Sql))
                            {
                                while (Reader.Read())
                                {
                                    report?.Invoke(false, 0);
                                    Writer.WriteLine("<REC>");
                                    int ColIndex = -1;
                                    foreach (var dic in fields)
                                    {
                                        ColIndex++;
                                        var key = dic.Key;
                                        var value = dic.Value;
                                        Writer.WriteLine($"<{(string.IsNullOrEmpty(value) ? key : value)}>={(Reader.IsDBNull(ColIndex) ? "" : Reader.GetValue(ColIndex).ToString())}");
                                    }
                                }
                            }
                        }

                        return (true, FileFullPath);
                    }
                }
                return (false, "暂无新数据！");
            }
            catch (Exception e)
            {
                return (false, e.Message + Environment.NewLine + e.StackTrace);
            }
        }
    }
}
