using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Xml.Linq;
using System.Xml;
using System.IO;

namespace DataDeliver
{
    public class MongoHelper
    {
        private static MongoClient _Client = null;
        private static string BaseDir { get; } = AppDomain.CurrentDomain.BaseDirectory;
        private static string RecordConfig { get; } = "RecordConfig.xml";

        static MongoHelper()
        {
            string MONGO_CONN = "mongodb://192.168.106.56";
            _Client = new MongoClient(MONGO_CONN);
        }

        /// <summary>
        /// 获取DB集合
        /// </summary>
        /// <returns></returns>
        internal static List<string> GetDBNames()
        {
            try
            {
                return _Client.ListDatabaseNames()?.ToList();
            }
            catch (Exception)
            {

                return null;
            }
        }

        /// <summary>
        /// 根据数据库名称获取集合
        /// </summary>
        /// <param name="dbName"></param>
        /// <returns></returns>
        internal static List<string> GetCollections(string dbName)
        {
            try
            {
                return _Client.GetDatabase(dbName).ListCollectionNames()?.ToList();
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 获取集合中的字段名称
        /// </summary>
        /// <param name="dbName"></param>
        /// <param name="collection"></param>
        /// <returns></returns>
        internal static HashSet<string> GetColumns(string dbName, string collection)
        {
            try
            {
                HashSet<string> results = new HashSet<string>();

                if (string.IsNullOrEmpty(dbName) || string.IsNullOrEmpty(collection))
                    return results;

                var Clt = _Client.GetDatabase(dbName).GetCollection<BsonDocument>(collection);

                var Datas = Clt.MapReduce(
                                                new BsonJavaScript(@"function() { for (var key in this) { emit(key, null); }}")
                                              , new BsonJavaScript(@"function(key, stuff) { return null; }")
                                              , new MapReduceOptions<BsonDocument, BsonDocument>() { Limit = 10000, OutputOptions = MapReduceOutputOptions.Inline, JavaScriptMode = true })?.ToList();

                if (Datas != null && Datas.Count > 1)
                {
                    foreach (var item in Datas)
                    {
                        var key = item.GetElement("_id").Value.ToString();
                        results.Add(key);
                    }
                }

                return results;
            }
            catch (Exception)
            {
                return null;
            }
        }

        /// <summary>
        /// 导出MongoDB数据
        /// </summary>
        /// <param name="dbName"></param>
        /// <param name="cltName"></param>
        /// <param name="fields"></param>
        /// <param name="baseDir"></param>
        /// <returns>(true/false,filePath/exception)</returns>
        internal static (bool, string) ExportData(string dbName, string cltName, Dictionary<string, string> fields, string baseDir, Action<bool, int> report)
        {
            try
            {
                var LastID = GetOrSetLastID(dbName, cltName);
                var Filter = string.IsNullOrEmpty(LastID) ? Builders<BsonDocument>.Filter.Empty : Builders<BsonDocument>.Filter.Gt("_id", new ObjectId(LastID));
                var Clt = _Client.GetDatabase(dbName).GetCollection<BsonDocument>(cltName);
                var DataCount = Clt.CountDocuments(Filter);
                if (DataCount > 0)
                {
                    var FileFullPath = Path.Combine(baseDir, $"{dbName}_{cltName}_{DateTime.Now.ToString("yyyyMMddHHmmss")}.txt");
                    report?.Invoke(true, (int)DataCount);
                    using (var Writer = new StreamWriter(FileFullPath, false, Encoding.Default))
                    {
                        using (var Cursor = Clt.FindSync(Filter, new FindOptions<BsonDocument> { NoCursorTimeout = true }))
                        {
                            while (Cursor.MoveNext())
                            {
                                foreach (var item in Cursor.Current)
                                {
                                    report?.Invoke(false, 0);
                                    LastID = item.GetElement("_id").Value.ToString();
                                    Writer.WriteLine("<REC>");
                                    foreach (var dic in fields)
                                    {
                                        var key = dic.Key;
                                        var value = dic.Value;
                                        Writer.WriteLine($"<{(string.IsNullOrEmpty(value) ? key : value)}>={item.GetValue(key, "")?.ToString() ?? ""}");
                                    }
                                }
                            }
                        }
                    }
                    // 记录最后一个ID
                    GetOrSetLastID(dbName, cltName, LastID);
                    return (true, FileFullPath);
                }
                return (false, "暂无新数据！");
            }
            catch (Exception e)
            {
                return (false, e.Message + Environment.NewLine + e.StackTrace);
            }
        }

        private static string GetOrSetLastID(string dbName, string cltName, string id = "")
        {
            XDocument xDoc = null;
            var FilePath = Path.Combine(BaseDir, RecordConfig);
            if (File.Exists(FilePath))
                xDoc = XDocument.Load(FilePath);
            else
            {
                xDoc = new XDocument(new XDeclaration("1.0", "utf-8", "yes"));
                xDoc.Add(new XElement("Root"));
            }




            var LastID = id;
            var TargetNode = from el in xDoc.Root.Elements()
                             where
                                el.Element("DbName").Value == dbName && el.Element("CltName").Value == cltName
                             select el;
            // 查找
            if (string.IsNullOrEmpty(id))
            {
                if (TargetNode == null || TargetNode.Count() == 0)
                { }
                else
                {
                    LastID = TargetNode.FirstOrDefault().Element("LastID").Value;
                }
            }
            // 更新
            else
            {
                if (TargetNode == null || TargetNode.Count() == 0)
                {
                    xDoc.Root.Add(new XElement("node",
                                        new XElement("DbName", dbName),
                                        new XElement("CltName", cltName),
                                        new XElement("LastID", id)));
                }
                else
                {
                    TargetNode.FirstOrDefault().Element("LastID").SetValue(id);
                }

                if (File.Exists(FilePath))
                    File.Delete(FilePath);
                xDoc.Save(Path.Combine(BaseDir, RecordConfig));
            }
            return LastID;
        }
    }
}
