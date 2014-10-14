using System;
using System.Collections.Generic;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace StarmanServiceIdMapper
{
    public class ServiceMap
    {
        [XmlAttribute]
        public string AdrenalinId { get; set; }
        [XmlAttribute]
        public int DvbServiceId { get; set; }

        [XmlIgnore]
        public string CubiwareId
        {
            get { return (DvbServiceId + 50000).ToString(CultureInfo.InvariantCulture); }
        }
    }

    internal class Program
    {
        private static Action<string> _log;
        private static readonly XNamespace Ns = "urn:tva:metadata:2010";
        private static string _logFile;

        private static void Log(string l)
        {
            if (_log != null)
                _log(l);
        }

        private static void LogToFile(string line)
        {
            var lines = new[] {string.Format("{0:yyyy-MM-dd HH:mm:ssZ} {1}", DateTime.UtcNow, line)};
            IoPersist(10, () => File.AppendAllLines(_logFile, lines));
        }

        public static void IoPersist(int tries, Action what)
        {
            while (true)
            {
                try
                {
                    what();
                    return;
                }
                catch (IOException)
                {
                    if (0 == --tries)
                        throw;
                    Thread.Sleep(500);
                }
            }
        }

        public static T IoPersist<T>(int tries, Func<T> what)
        {
            while (true)
            {
                try
                {
                    return what();
                }
                catch (IOException)
                {
                    if (0 == --tries)
                        throw;
                    Thread.Sleep(500);
                }
            }
        }

        private static int Main()
        {
            _logFile = ConfigurationManager.AppSettings["LogFile"];
            _log = string.IsNullOrEmpty(_logFile)
                ? (Action<string>) Console.WriteLine
                : l =>
                {
                    LogToFile(l);
                    Console.WriteLine(l);
                };

            var mappingFile = ConfigurationManager.AppSettings["MappingFile"];
            if (!File.Exists(mappingFile))
            {
                var map = new[] {new ServiceMap {AdrenalinId = "53001", DvbServiceId = 1011}};
                XmlHelper<ServiceMap[]>.ToFile(map, mappingFile);
            }
            var mappingSrc = XmlHelper<ServiceMap[]>.FromFile_Persist(mappingFile);
            var mapping = mappingSrc.ToDictionary(sm => sm.AdrenalinId);

            var outputFolder = ConfigurationManager.AppSettings["OutputFolder"];
            Directory.CreateDirectory(outputFolder);
            var tempFolder = ConfigurationManager.AppSettings["TempFolder"];
            Directory.CreateDirectory(tempFolder);

            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["InputURI"]))
            {
                var tempPath = Path.Combine(tempFolder, "FullTVA.xml");
                var outputPath = Path.Combine(outputFolder, "FullTVA.xml");
                Go(ConfigurationManager.AppSettings["InputURI"], mapping, tempPath, outputPath);
            }
            else
            {
                var inputFolder = ConfigurationManager.AppSettings["InputFolder"];
                Directory.CreateDirectory(inputFolder);
                var inputFiles = Directory.GetFiles(inputFolder, "*.xml");
                foreach (var inputPath in inputFiles)
                {
                    var inputFile = Path.GetFileName(inputPath) ?? inputPath;
                    var tempPath = Path.Combine(tempFolder, inputFile);
                    var outputPath = Path.Combine(outputFolder, inputFile);
                    Go(inputPath, mapping, tempPath, outputPath);
                }
            }
            return 0;
        }

        private static void Go(string inputFile, Dictionary<string, ServiceMap> mapping, string tempPath, string outputPath)
        {
            Log("Loading " + inputFile);
            var doc = TryRetry(10, () => XDocument.Load(inputFile), "Loading TVA from " + inputFile);

            Log("Mapping ServiceIds");
            MapServiceIds(doc, mapping);

            Log("Saving to " + tempPath);
            doc.Save(tempPath);

            if (File.Exists(outputPath))
                TryRetry(10, () => File.Delete(outputPath), "Deleting existing file " + outputPath);

            Log("Moving to " + outputPath);
            File.Move(tempPath, outputPath);

            if (inputFile.StartsWith("http"))
                return;

            Log("Deleting input file " + inputFile);
            TryRetry(10, () => File.Delete(inputFile), "Deleting input file " + inputFile);
        }

        private static void MapServiceIds(XDocument doc, Dictionary<string, ServiceMap> mapping)
        {
            var plt = doc.Descendants(Ns + "ProgramLocationTable").SingleOrDefault();
            if (plt != null)
            {
                var scheduleServiceIdRefs =
                    plt.Elements(Ns + "Schedule").Select(se => se.Attributes("serviceIDRef").Single());
                foreach( var attr in scheduleServiceIdRefs)
                    MapServiceIdAttribute(attr, mapping);

                var broadcastEventServiceIdRefs =
                                        plt.Elements(Ns + "BroadcastEvent").Select(se => se.Attributes("serviceIDRef").Single());
                foreach (var attr in broadcastEventServiceIdRefs)
                    MapServiceIdAttribute(attr, mapping);

            }
            var sit = doc.Descendants(Ns + "ServiceInformationTable").SingleOrDefault();
            if (sit != null)
            {
                var serviceElements = sit.Elements(Ns + "ServiceInformation");

                foreach (var siElement in serviceElements)
                {
                    var serviceIdAttr = siElement.Attributes("serviceId").Single();
                    var adrenalinServiceId = serviceIdAttr.Value;
                    ServiceMap serviceMap;
                    if (!mapping.TryGetValue(adrenalinServiceId, out serviceMap))
                    {
                        Log("Warning: Unmapped 'AdrenalinId' " + adrenalinServiceId);
                        continue;
                    }

                    Log(string.Format("Changing serviceId {0} to {1}", adrenalinServiceId, serviceMap.CubiwareId));
                    serviceIdAttr.Value = serviceMap.CubiwareId;

                    var dvbServiceId = serviceMap.DvbServiceId.ToString(CultureInfo.InvariantCulture);

                    Log(string.Format("Adding ServiceGenre for {0} to {1}", serviceMap.CubiwareId, serviceMap.DvbServiceId));
                    var customPropElement = new XElement(Ns + "ServiceGenre",
                                                    new object[]
                                                        {
                                                            new XAttribute("href", "urn:starman:2013:dvbServiceId"),
                                                            new XAttribute("type", "other"),
                                                            new XElement(Ns + "Definition", new XText(dvbServiceId))
                                                        });
                    var insertPoint = siElement.Elements(Ns + "Name").LastOrDefault();

                    if (insertPoint != null)
                        insertPoint.AddAfterSelf(customPropElement);
                    else
                        siElement.AddFirst(customPropElement);
                }
            }
        }

        private static void MapServiceIdAttribute(XAttribute attr, Dictionary<string, ServiceMap> mapping)
        {
            ServiceMap serviceMap;
            if (mapping.TryGetValue(attr.Value, out serviceMap))
                attr.Value = serviceMap.CubiwareId;
        }

        private static T TryRetry<T>(int tries, Func<T> func, string msg)
        {
            var q = 0;
            while (true)
            {
                try
                {
                    var retVal = func();
                    Log(msg + " OK");
                    return retVal;
                }
                catch (Exception e)
                {
                    if (++q >= tries)
                    {
                        Log(string.Format("Really failed {1}: {0}", e.Message, msg));
                        throw;
                    }
                    Log(string.Format("Failed {1}: {0}, retrying", e.Message, msg));
                    Thread.Sleep(50);
                }
            }
        }

        private static void TryRetry(int tries, Action func, string msg)
        {
            var q = 0;
            while (true)
            {
                try
                {
                    func();
                    Log(msg + " OK");
                    return;
                }
                catch (Exception e)
                {
                    if (++q >= tries)
                    {
                        Log(string.Format("Really failed to {1}: {0}", e.Message, msg));
                        throw;
                    }
                    Log(string.Format("Failed to {1}: {0}, retrying", e.Message, msg));
                    Thread.Sleep(50);
                }
            }
        }
    }
}
