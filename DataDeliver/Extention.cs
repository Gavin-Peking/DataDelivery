using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataDeliver
{
    public static class Extention
    {
        public static string RemoveInvalidSpace(this string text)
        {
            if (string.IsNullOrEmpty(text))
                return "";
            return text.Replace("\u0009", " ")
                      .Replace("\u000A", " ")
                      .Replace("\u000B", " ")
                      .Replace("\u000C", " ")
                      .Replace("\u000D", " ")
                      .Replace("\u0020", " ")
                      .Replace("\u0085", " ")
                      .Replace("\u00A0", " ")
                      .Replace("\u1680", " ")
                      .Replace("\u180E", " ")
                      .Replace("\u2000", " ")
                      .Replace("\u2001", " ")
                      .Replace("\u2002", " ")
                      .Replace("\u2003", " ")
                      .Replace("\u2004", " ")
                      .Replace("\u2005", " ")
                      .Replace("\u2006", " ")
                      .Replace("\u2007", " ")
                      .Replace("\u2008", " ")
                      .Replace("\u2009", " ")
                      .Replace("\u200A", " ")
                      .Replace("\u200B", " ")
                      .Replace("\u200C", " ")
                      .Replace("\u200D", " ")
                      .Replace("\u2028", " ")
                      .Replace("\u2029", " ")
                      .Replace("\u202F", " ")
                      .Replace("\u205F", " ")
                      .Replace("\u2060", " ")
                      .Replace("\u3000", " ")
                      .Replace("\uFEFF", " ").Replace("", " ");// 最后一个replace 方法第一个参数是‘\u8’字符，误删
        }
    }
}
