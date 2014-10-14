using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml.Serialization;

namespace ClassifyContentLib
{
    public static class XmlHelper<T>
        where T : class
    {
        public static T FromFile(string path)
        {
            using (var stringReader = new System.IO.FileStream(path, System.IO.FileMode.Open, System.IO.FileAccess.Read)
                )
            {
                return (new XmlSerializer(typeof(T))).Deserialize(stringReader) as T;
            }
        }

        public static T FromString(string xml)
        {
            if (xml == null)
                return null;

            using (var stringReader = new System.IO.StringReader(xml))
            {
                return (new XmlSerializer(typeof(T))).Deserialize(stringReader) as T;
            }
        }

        public static string ToString(T obj)
        {
            using (var stringWriter = new System.IO.StringWriter())
            {
                new XmlSerializer(typeof(T)).Serialize(stringWriter, obj);
                stringWriter.Flush();
                return stringWriter.ToString();
            }
        }

        public static void ToFile(T obj, string path)
        {
            using (var stringWriter = new System.IO.FileStream(path, System.IO.FileMode.Create, System.IO.FileAccess.Write))
            {
                var xs = new XmlSerializer(typeof(T));
                xs.Serialize(stringWriter, obj);
                stringWriter.Flush();
            }
        }
    }

    public class FileEntry
    {
        public FileEntry()
        {
            TraxisStatus = TraxisState.Unknown;
            ConvoyStatus = ConvoyState.Unknown;
        }

        public ConvoyState ConvoyStatus { get; set; }

        public string DateString { get; set; }
        public string TimeString { get; set; }
        public string SizeString { get; set; }
        public string FileName { get; set; }

        public enum ConvoyState
        {
            Unknown,
            NotFound,
            Found,
            Error,
            Initial
        }

        public enum TraxisState
        {
            Unknown,
            NotFound,
            Unavailable,
            Vod,
            Catchup,
            Npvr,
        }

        public TraxisState TraxisStatus { get; set; }

        public string ChannelName { get; set; }

        static readonly CultureInfo CultureEnUs = new CultureInfo("en-US");

        public long Size
        {
            get
            {
                var numberOnly = SizeString.Split(new[] {'G', 'M', 'K'})[0];
                var number = double.Parse(numberOnly, CultureEnUs);
                if (SizeString.EndsWith("G"))
                    number *= 1024*1024*1024;
                else if (SizeString.EndsWith("M"))
                    number *= 1024*1024;
                else if (SizeString.EndsWith("K"))
                    number *= 1024;
                return (long) number;
            }
        }

        public string VodBackofficeId
        {
            get
            {
                var fn = System.IO.Path.GetFileNameWithoutExtension(FileName);
                return fn != null && fn.Length == 36 ? fn : null;
            }
        }

        public DateTime FileDate { get { throw new NotImplementedException(); } }

        public long DurationInMinutes { get; set; }
    }

    class Program
    {
        private static TextWriter _output;

        private static void Main(string[] args)
        {
            if (args.Any())
                _output = new StreamWriter(args[0]);

            var guidFileEntries = ReadGuidFileEntries();

            var cts = new CancellationTokenSource();
            var cancelToken = cts.Token;

            try
            {
                var traxisTask = Task.Factory.StartNew(() => PopulateTraxisStatuses(guidFileEntries, cancelToken),
                                                       cancelToken);

                var omsTask = Task.Factory.StartNew(() => PopulateEdgewareStatuses(guidFileEntries, cancelToken),
                                                    cancelToken);

                var allTasks = new[] {traxisTask, omsTask};
                while (!Task.WaitAll(allTasks, 50))
                {
                    if (!Console.KeyAvailable)
                        continue;
                    cts.Cancel();
                    Console.ReadKey();
                    WriteLine("Waiting for loop to actually exit");
                }
            }
            catch (OperationCanceledException)
            {
            }

            CleanupChannelNames(guidFileEntries);

            var partialDownloads = guidFileEntries.Where(fe => fe.FileName.EndsWith(".part")).ToArray();
            var notInTraxisEntries =
                guidFileEntries.Where(e => e.TraxisStatus == FileEntry.TraxisState.NotFound).ToArray();
            WriteSummary(guidFileEntries, "with GUID as filename (includes PART files)");
            WriteSummary(partialDownloads, "PARTIALLY downloaded files with GUID");

            WriteSummary(guidFileEntries.Where(e => e.ConvoyStatus == FileEntry.ConvoyState.NotFound).ToArray(),
                         "Files on NAS but unknown by Convoy");
            WriteSummary(
                guidFileEntries.Where(
                    e =>
                    e.ConvoyStatus == FileEntry.ConvoyState.NotFound && e.TraxisStatus != FileEntry.TraxisState.NotFound)
                               .ToArray(),
                "Files on NAS + Traxis but unknown by Convoy");
            WriteSummary(
                guidFileEntries.Where(
                    e =>
                    e.ConvoyStatus == FileEntry.ConvoyState.NotFound && e.TraxisStatus == FileEntry.TraxisState.NotFound)
                               .ToArray(),
                "Files on NAS + but unknown by Convoy and Traxis");
            WriteSummary(
                partialDownloads.Where(
                    e =>
                    e.ConvoyStatus == FileEntry.ConvoyState.NotFound && e.TraxisStatus == FileEntry.TraxisState.NotFound)
                                .ToArray(),
                "of which are Partials files");
            WriteSummary(notInTraxisEntries,
                         "NotFound in Traxis");
            WriteSummary(guidFileEntries.Where(e => e.TraxisStatus == FileEntry.TraxisState.Unavailable).ToArray(),
                         "Currently marked Unavailable in Traxis");
            WriteSummary(guidFileEntries.Where(e => e.TraxisStatus == FileEntry.TraxisState.Vod).ToArray(),
                         "VoD in Traxis");
            WriteSummary(guidFileEntries.Where(e => e.TraxisStatus == FileEntry.TraxisState.Catchup).ToArray(),
                         "CatchUp in Traxis");
            WriteSummary(guidFileEntries.Where(e => e.TraxisStatus == FileEntry.TraxisState.Npvr).ToArray(),
                         "nPVR in Traxis");
            WriteSummary(guidFileEntries.Where(e => e.TraxisStatus == FileEntry.TraxisState.Unknown).ToArray(),
                         "Traxis status unknown");

            var activeLibrary = guidFileEntries.Where(e => e.TraxisStatus == FileEntry.TraxisState.Vod ||
                                                           e.TraxisStatus == FileEntry.TraxisState.Catchup ||
                                                           e.TraxisStatus == FileEntry.TraxisState.Npvr).ToArray();

            WriteSummary(activeLibrary, "Total Active in Traxis");
            var partialButActive = activeLibrary.Intersect(partialDownloads).ToArray();
            WriteSummary(partialButActive, "Total Active in Traxis, but partially downloaded");
            WriteSummary(partialButActive.Where(e => e.TraxisStatus == FileEntry.TraxisState.Vod).ToArray(),
                         "Total Active in Traxis, but partially downloaded, and VoD");

            {
                var cuEntries = guidFileEntries.Where(_ => _.TraxisStatus == FileEntry.TraxisState.Catchup).ToArray();
                var cuChannelNames =
                    cuEntries.Where(_ => _.ChannelName != null).Select(_ => _.ChannelName).Distinct().OrderBy(_ => _).
                              ToArray();
                foreach (var channelName in cuChannelNames)
                    WriteSummary(
                        cuEntries.Where(e => e.ChannelName != null && e.ChannelName.Equals(channelName)).ToArray(),
                        "Catchup channel " + channelName);
            }

            {
                var npvrEntries = guidFileEntries.Where(_ => _.TraxisStatus == FileEntry.TraxisState.Npvr).ToArray();
                var npvrChannelNames =
                    npvrEntries.Where(_ => _.ChannelName != null).Select(_ => _.ChannelName).Distinct().OrderBy(_ => _).
                                ToArray();
                foreach (var channelName in npvrChannelNames)
                    WriteSummary(
                        npvrEntries.Where(e => e.ChannelName != null && e.ChannelName.Equals(channelName)).ToArray(),
                        "nPVR channel " + channelName);
            }

            {
                var partialsByConvoyStatus = partialDownloads.GroupBy(_ => _.ConvoyStatus).ToArray();
                foreach (var statusGroup in partialsByConvoyStatus)
                    WriteSummary(statusGroup.AsEnumerable().ToArray(),
                                 "Partial Downloads with ConvoyStatus " + statusGroup.Key);
            }

            {
                var notInTraxisEntriesByConvoyStatus = notInTraxisEntries.GroupBy(_ => _.ConvoyStatus).ToArray();
                foreach (var statusGroup in notInTraxisEntriesByConvoyStatus)
                    WriteSummary(statusGroup.AsEnumerable().ToArray(),
                                 "NotInTraxis Files with ConvoyStatus " + statusGroup.Key);
            }

            Console.WriteLine("Press any key to start Writing XML");
            Console.ReadLine();
            XmlHelper<FileEntry[]>.ToFile(guidFileEntries, @"StarmanConvoyEntries.xml");
            _output.Close();
            _output.Dispose();
        }

        static readonly XNamespace CpiNs = "urn:eventis:cpi:1.0";
        private static void PopulateEdgewareStatuses(IEnumerable<FileEntry> entires, CancellationToken cancelToken)
        {
            try
            {
                WriteLine("Downloading fresh ConvoyContents");
                //throw new Exception();
                new WebClient().DownloadFile(@"http://ew-oms.starman.ee:8080/nordbro/3d/Contents",
                                             @"StarmanConvoyContents.xml_temp");
                File.Delete(@"StarmanConvoyContents.xml");
                File.Move(@"StarmanConvoyContents.xml_temp", @"StarmanConvoyContents.xml");
                WriteLine("Downloaded fresh ConvoyContents");
            }
            catch
            {
                WriteLine("Didn't download fresh ConvoyContents");
            }
            try
            {
                var allContentsDoc = XDocument.Load(@"StarmanConvoyContents.xml");
                var statusDic = allContentsDoc.Descendants(CpiNs + "Content").ToDictionary(e => e.Attribute("id").Value,
                                                                                           e =>
                                                                                           e.Attribute("statusId").Value);
                Parallel.ForEach(entires, new ParallelOptions {CancellationToken = cancelToken}, entry =>
                    {
                        string statusId;
                        if (!statusDic.TryGetValue(entry.VodBackofficeId, out statusId))
                            entry.ConvoyStatus = FileEntry.ConvoyState.NotFound;
                        else if (statusId.StartsWith("Ingest"))
                            entry.ConvoyStatus = FileEntry.ConvoyState.Found;
                        else if (statusId.StartsWith("Initial"))
                            entry.ConvoyStatus = FileEntry.ConvoyState.Initial;
                        else
                            entry.ConvoyStatus = FileEntry.ConvoyState.Error;
                    });
            }
            catch (OperationCanceledException)
            {
            }
            WriteLine("Convoy Done");
        }

        private static void PopulateTraxisStatuses(IEnumerable<FileEntry> guidFileEntries, CancellationToken cancelToken)
        {
            try
            {
                Parallel.ForEach(guidFileEntries, new ParallelOptions {CancellationToken = cancelToken},
                                 PopulateTraxisStatus);
            }
            catch (OperationCanceledException)
            {
            }
            WriteLine("Traxis Done");
        }

        private static void WriteLine(string what, object arg0 = null, object arg1=null)
        {
            var msg = string.Format(what, arg0, arg1);
            Console.WriteLine(msg);
            if (_output != null)
                _output.WriteLine(msg);
        }

        private static void CleanupChannelNames(IEnumerable<FileEntry> guidFileEntries)
        {
            Parallel.ForEach(guidFileEntries.Where(_ => _.ChannelName != null), fe =>
                {
                    fe.ChannelName = fe.ChannelName.Replace("NPVR-", "");
                    fe.ChannelName = fe.ChannelName.Replace(" ", "");
                });
        }

        private static void WriteSummary(FileEntry[] entries, string desc)
        {
            WriteLine(string.Format("{0} {2}, total {1} Gb, {3} minutes", entries.Length,
                              entries.Sum(_ => _.Size)/(1024*1024*1024), desc, entries.Sum(_ => _.DurationInMinutes)));
        }

        private static FileEntry[] ReadGuidFileEntries()
        {
            return System.IO.File.Exists(@"starmanconvoyentries.xml") ? GuidFileEntriesFromXml() : GuidFileEntriesFromListing();
        }

        private static FileEntry[] GuidFileEntriesFromXml()
        {
            return XmlHelper<FileEntry[]>.FromFile(@"starmanconvoyentries.xml");
        }

        private static FileEntry[] GuidFileEntriesFromListing()
        {
            var lines = System.IO.File.ReadAllLines(@"starmanconvoylist.txt");
            var split = lines.Select(line => line.Split(new[] {' '}, StringSplitOptions.RemoveEmptyEntries)).ToList();
            WriteLine(split.Count + " lines");
            var split9 = split.Where(_ => _.Length == 9).ToList();
            WriteLine(split9.Count + " of length 9");
            var fileEntries =
                split9.Select(
                    arr =>
                    new FileEntry
                        {
                            SizeString = arr[4],
                            DateString = arr[5] + " " + arr[6],
                            TimeString = arr[7],
                            FileName = arr[8]
                        }).ToList();
            var guidFileEntries = fileEntries.Where(fe => fe.VodBackofficeId != null).ToArray();
            return guidFileEntries;
        }

        static readonly XNamespace TraxisNs = "urn:eventis:traxisweb:1.0";

        static void PopulateTraxisStatus(FileEntry entry)
        {
            if (entry.TraxisStatus == FileEntry.TraxisState.NotFound)
                return;

            if (entry.TraxisStatus != FileEntry.TraxisState.Unknown)
                return;

            entry.ChannelName = null;

            const string traxisUrlTemplate = @"http://traxis.starman.ee/traxis/web/Content/{0}/props/DurationInSeconds,Products,TsTv,FirstAvailability,LastAvailability,Titles?AliasIdType=VodBackOfficeId&method=POST&body={1}";
            const string body = @"<SubQueryOptions><QueryOption path='Products'>/props/Name</QueryOption><QueryOption path='Titles'>/props/DurationInSeconds,Events,Categories</QueryOption><QueryOption path='Titles/Categories'>/props/Name</QueryOption><QueryOption path='Titles/Events'>/props/Channels,TsTvContents</QueryOption><QueryOption path='Titles/Events/Channels'>/props/Name</QueryOption></SubQueryOptions>";
            var encodedBody = Uri.EscapeDataString(body);
            var url = string.Format(traxisUrlTemplate, entry.VodBackofficeId, encodedBody);
            try
            {
                var doc = XDocument.Load(url);
                entry.TraxisStatus = GetTraxisStatus(doc, entry);
            }
            catch (Exception e)
            {
                entry.TraxisStatus = FileEntry.TraxisState.NotFound;
                WriteLine("Error {0}: {1}", entry.VodBackofficeId, e.Message);
            }
        }

        private static FileEntry.TraxisState GetTraxisStatus(XDocument doc, FileEntry entry)
        {
            try
            {
                entry.DurationInMinutes =
                    long.Parse(doc.Descendants(TraxisNs + "DurationInSeconds").Select(_ => _.Value).First())/60;
            } catch
            {
                WriteLine("No DurationInSeconds");
            }

            if (!IsAvailableNow(doc))
                return FileEntry.TraxisState.Unavailable;

            var productNames = doc.Descendants(TraxisNs + "Product").SelectMany(
                chanElement => chanElement.Elements(TraxisNs + "Name"))
                .Select(_ => _.Value).ToArray();

            if (!productNames.Any())
                return FileEntry.TraxisState.Unavailable;

            var notCatchup = !productNames.Any(ProductNameIndicatesCatchUp);
            if (notCatchup)
                WriteLine("Not Catchup " + string.Concat(productNames));

            var channelNames = doc.Descendants(TraxisNs + "Channel").SelectMany(
                chanElement => chanElement.Elements(TraxisNs + "Name").Select(_ => _.Value));
            entry.ChannelName = channelNames.FirstOrDefault();

            if (!string.IsNullOrEmpty(entry.ChannelName))
                return notCatchup ? FileEntry.TraxisState.Npvr : FileEntry.TraxisState.Catchup;

            var tstvElement = doc.Descendants(TraxisNs + "Tstv").FirstOrDefault();
            if (tstvElement != null)
            {
                var categoryNames = doc.Descendants(TraxisNs + "Category").SelectMany(
                    chanElement => chanElement.Elements(TraxisNs + "Name").Select(_ => _.Value));
                entry.ChannelName = entry.ChannelName ?? categoryNames.FirstOrDefault() ?? productNames.FirstOrDefault() ??
                                    "UND";
                return FileEntry.TraxisState.Npvr;
            }
            return FileEntry.TraxisState.Vod;
        }

        private static bool ProductNameIndicatesCatchUp(string arg)
        {
            return arg.StartsWith("CU", StringComparison.InvariantCultureIgnoreCase) ||
                   arg.EndsWith("CU", StringComparison.InvariantCultureIgnoreCase) ||
                   arg.EndsWith("CUTV", StringComparison.InvariantCultureIgnoreCase);
        }

        private static bool IsAvailableNow(XDocument doc)
        {
            try
            {
                var first = DateTime.Parse(doc.Descendants(TraxisNs + "FirstAvailability").First().Value, null,
                                           DateTimeStyles.RoundtripKind);
                var last = DateTime.Parse(doc.Descendants(TraxisNs + "LastAvailability").First().Value, null,
                                          DateTimeStyles.RoundtripKind);
                var now = DateTime.UtcNow;
                return first <= now && last >= now;
            }
            catch (Exception)
            {
                Console.Error.WriteLine("Can't determine Availability, saying Yes");
                if (_output != null)
                    _output.WriteLine("Can't determine Availability, saying Yes");
                return true;
            }
        }
    }
}
