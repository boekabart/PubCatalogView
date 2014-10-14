using System;
using System.IO;
using System.Xml.Serialization;

namespace StarmanServiceIdMapper
{
    public static class XmlHelper<T>
        where T : class
    {
        public static T FromFile_Persist(string path)
        {
            return Program.IoPersist(10, () => FromFile(path));
        }

        public static T FromFile(string path)
        {
            using (var stringReader = new FileStream(path, FileMode.Open, FileAccess.Read)
                )
            {
                return (new XmlSerializer(typeof (T))).Deserialize(stringReader) as T;
            }
        }

        public static T FromString(string xml)
        {
            if (xml == null)
                return null;

            using (var stringReader = new StringReader(xml))
            {
                return (new XmlSerializer(typeof (T))).Deserialize(stringReader) as T;
            }
        }

        public static string ToString(T obj)
        {
            using (var stringWriter = new StringWriter())
            {
                new XmlSerializer(typeof (T)).Serialize(stringWriter, obj);
                stringWriter.Flush();
                return stringWriter.ToString();
            }
        }

        public static void ToFile(T obj, string path)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(path));
            try
            {

                using (var stream = new FileStream(path, FileMode.Create))
                {
                    new XmlSerializer(typeof (T)).Serialize(stream, obj);
                    stream.Flush();
                }

            }
            catch (Exception)
            {
                if (File.Exists(path))
                    File.Delete(path);
                throw;
            }
        }
    }
}
