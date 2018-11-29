using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TPI;
using System.IO;


namespace DataDeliver
{
    public class KbaseHelper
    {
        private static string ConnIp { get; set; } = "192.168.106.60";
        private static int Port { get; set; } = 4567;
        private static string UserName { get; set; } = "DBOWN";

        internal static List<string> GetCols(string tableName)
        {
            List<string> Results = new List<string>();
            var Sql = $"SELECT * FROM {tableName} limit 1";

            using (var Client = new Client())
            {
                if (Client.Connect(ConnIp, Port, UserName, ""))
                {
                    var rs = Client.OpenRecordSet(Sql);
                    if (rs == null)
                        return Results;

                    for (int i = 0; i < rs.GetFieldCount(); i++)
                    {
                        var name = rs.GetFieldName(i);
                        Results.Add(name);
                    }
                }
                // 安全起见
                Client.Close();
                Client.Dispose();
            }
            return Results;
        }

        internal static (bool, string) ExportData(string tableName, string where, Dictionary<string, string> fields, string baseDir, Action<int> report)
        {
            StringBuilder SqlBuilder = new StringBuilder();
            SqlBuilder.Append("SELECT ");
            SqlBuilder.Append(fields.Keys.Aggregate((f1, f2) => f1 + "," + f2));
            SqlBuilder.Append($" FROM {tableName} ");
            if (!string.IsNullOrEmpty(where))
            {
                SqlBuilder.Append($"{(where.ToLower().Contains("where") ? "" : "WHERE")} {where}");
            }

            try
            {
                using (var Client = new Client())
                {
                    if (Client.Connect(ConnIp, Port, UserName, ""))
                    {
                        var rs = Client.OpenRecordSet(SqlBuilder.ToString());
                        report?.Invoke(0);

                        var FileFullPath = Path.Combine(baseDir, $"{tableName}_{DateTime.Now.ToString("yyyyMMddHHmmssfff")}.txt");
                        using (var Writer = new StreamWriter(FileFullPath, false, Encoding.Default))
                        {
                            int Cnt = 0;
                            while (rs.MoveNext())
                            {
                                Cnt++;
                                Writer.WriteLine("<REC>");
                                foreach (var dic in fields)
                                {
                                    var key = dic.Key;
                                    var value = dic.Value;
                                    Writer.WriteLine($"<{(string.IsNullOrEmpty(value) ? key : value)}>={rs.GetValue(key)?.ToString() ?? ""}");
                                }
                                report(Cnt);
                            }
                        }

                        // 安全起见
                        Client.Close();
                        Client.Dispose();
                        return (true, FileFullPath);
                    }
                    // 安全起见
                    Client.Close();
                    Client.Dispose();

                    return (false,"Kbase 连接失败...");
                }

            }
            catch (Exception e)
            {
                return (false, e.Message);
            }
        }

        internal static (bool, string) UploadData(string tableName, string fileName)
        {
            // Z:\RecFiles\机标关键词
            try
            {
                var FilePath = Path.Combine(@"Z:\RecFiles\机标关键词", fileName);
                var Sql = $@"DBUM UPDATE TABLE {tableName}
	                            FROM  '{FilePath}'
	                            WITH KEY 文件名";
                using (var Client = new Client())
                {
                    if (Client.Connect(ConnIp, Port, UserName, ""))
                    {
                        Client.ExecMgrSQL(Sql);
                        // 安全起见
                        Client.Close();
                        Client.Dispose();
                        return (true, string.Empty);
                    }
                    // 安全起见
                    Client.Close();
                    Client.Dispose();
                    return (false,"Kbase 连接失败...");
                }
               
            }
            catch (Exception e)
            {
                return (false, e.Message);
            }

            throw new NotImplementedException();
        }

        public static void Dispose()
        { }
    }
}
