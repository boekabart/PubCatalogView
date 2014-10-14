using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace CountConcurrentSessionsPerAsset
{
    public class DownloadMoment
    {
        public DateTime DownloadTime;
        public Asset Asset;
        public Int64 TotalMemoryInUse;

        public DownloadMoment()
        {

        }

        public DownloadMoment(DateTime time, Asset asset, Int64 totalMemoryInUse)
        {
            DownloadTime = time;
            Asset = asset;
            TotalMemoryInUse = totalMemoryInUse;
        }

        public DateTime EndTime { get { return DownloadTime + DownloadDuration; } }
        public TimeSpan DownloadDuration { get { return TimeSpan.FromSeconds((Asset.Filesize*8.0)/Asset.DownloadBitrate); } }
    }

    public class Period
    {
        public DateTime StartTime;
        public DateTime EndTime;

        public TimeSpan Duration
        {
            get { return EndTime - StartTime; }
        }

        public bool Intersects(Period rhs)
        {
            return (StartTime < rhs.EndTime && EndTime > rhs.StartTime) ||
                   (rhs.StartTime < EndTime && rhs.EndTime < rhs.StartTime);
        }

        public static Period FromDayLocal(DateTime src)
        {
            return new Period
                {
                    StartTime = src.ToLocalTime().Date,
                    EndTime = src.ToLocalTime().Date.AddDays(1),
                };
        }

        public bool Contains(DateTime rhs)
        {
            return rhs >= StartTime && rhs < EndTime;
        }
    }

    public class Session : Period
    {
        public string AssetId;

        public Asset Asset
        {
            get { return Asset.Dictionary[AssetId]; }
        }
    }

    public class Asset
    {
        public string Id;
        public Int64 Bitrate;
        public Int64 Duration;

        public bool RestartTv
        {
            get { return RecordingStart.HasValue; }
        }

        public DateTime? RecordingStart;

        public Int64 Filesize
        {
            get { return Duration > 0 && Bitrate > 0 ? Duration*Bitrate/8 : AverageFilesize; }
        }

        public static Int64 DownloadBitrate = 20000000;

        public static ConcurrentDictionary<string, Asset> Dictionary = new ConcurrentDictionary<string, Asset>();

        public static void FillDictionary(IEnumerable<string> assetIds)
        {
            var cacheFile = @"d:\temp\Starman\AssetInfoCache.xml";
            try
            {
                var assets = XmlHelper<Asset[]>.FromFile(cacheFile);
                foreach (var asset in assets)
                    Dictionary.TryAdd(asset.Id, asset);
                Console.WriteLine("Asset Cache read");
            }
            catch (Exception)
            {
            }
            Parallel.ForEach(assetIds.Distinct().OrderBy(_ => _), AddAssetToDictionary);
            Console.WriteLine("Writing Asset Cache");
            XmlHelper<Asset[]>.ToFile(Dictionary.Values.ToArray(), cacheFile);
            AverageFilesize = (Int64) Dictionary.Values.Select(_ => _.Filesize).Where(_ => _ > 0).Average();
        }

        private static void AddAssetToDictionary(string assetId)
        {
            Dictionary.AddOrUpdate(assetId, RetrieveAsset, (_, __) => __);
        }

        private static readonly XNamespace TraxisNs = "urn:eventis:traxisweb:1.0";
        private static Int64 AverageFilesize;

        private static Asset RetrieveAsset(string assetId)
        {
            var fileName = string.Format(@"d:\temp\Starman\AssetInfo\{0}.xml", assetId);
            if (!File.Exists(fileName))
            {
                try
                {
                    const string template =
                        "http://traxis.starman.ee/traxis/web/Content/{0}/props/Tstv,DurationInSeconds,FirstAvailability,MaxBitrateInBps?aliasidtype=VodBackOfficeId";
                    var uri = string.Format(template, assetId);

                    using (var wc = new WebClient())
                        wc.DownloadFile(uri, fileName);
                    Console.WriteLine("Downloaded");
                }
                catch
                {
                    File.WriteAllBytes(fileName, new byte[0]);
                }
            }

            try
            {
                var doc = XDocument.Load(fileName);
                Console.WriteLine("Read {0}", assetId);
                var restartTv =
                    doc.Descendants(TraxisNs + "Option").Attributes("model").Any(att => att.Value.Equals("Delay"));
                DateTime? recStart = null;
                if (restartTv)
                    recStart =
                        DateTime.Parse(doc.Descendants(TraxisNs + "FirstAvailability").Single().Value, null,
                                       DateTimeStyles.RoundtripKind).ToLocalTime();

                return new Asset
                    {
                        Id = assetId,
                        Bitrate = Int64.Parse(doc.Descendants(TraxisNs + "MaxBitrateInBps").Single().Value),
                        Duration = Int64.Parse(doc.Descendants(TraxisNs + "DurationInSeconds").Single().Value),
                        RecordingStart = recStart,
                    };
            }
            catch
            {
                Console.WriteLine("Not found asset {0}", assetId);
                return new Asset {Id = assetId};
            }
        }

        public static Int64 TotalFilesize(IEnumerable<string> assetIds)
        {
            return assetIds.Distinct().Select(id => Dictionary[id]).Sum(_ => _.Filesize);
        }
    }

    internal class Program
    {
        private static void Main(string[] args)
        {
            string vodUsageFolder = @"D:\temp\Starman\StarmanVodUsage20130828_20130905";

            var outputRootFolder = Path.Combine(vodUsageFolder, "Analysis");

            var completedSessionsEnum =
                GetCompletedSessionsFromVodUsageFolder(vodUsageFolder);
            Console.WriteLine("Read files");
            var completedSessions = completedSessionsEnum.ToArray();
            Console.WriteLine("Read {0} Sessions", completedSessions.Length);

            Console.WriteLine("Reading asset data");
            Asset.FillDictionary(completedSessions.Select(ses => ses.AssetId));
            Console.WriteLine("Read asset data");

            PerformFullPeriodAnalysis(completedSessions, outputRootFolder);
            PerformPerDayAnalysis(completedSessions, outputRootFolder);
            AnalyseDownloadTimesAndSave(completedSessions, outputRootFolder);
        }

        private static void PerformFullPeriodAnalysis(Session[] completedSessions, string outputRootFolder)
        {
            Console.WriteLine("Performing full period analysis");
            var fullPeriod = GetFullPeriod(completedSessions);
            var periodAnalysisOutputFolder = outputRootFolder;
            Directory.CreateDirectory(periodAnalysisOutputFolder);
            AnalyseSessions(completedSessions, fullPeriod, periodAnalysisOutputFolder);
        }

        private static void PerformPerDayAnalysis(Session[] completedSessions, string outputRootFolder)
        {
            var fullDays = GetFullDays(completedSessions);
            foreach (var day in fullDays)
            {
                var dayString = day.StartTime.ToString("yyyy-MM-dd");
                var sessionsTouchingTheDay = completedSessions.Where(ses => ses.Intersects(day)).ToArray();
                var sessionsStartingTheDay =
                    sessionsTouchingTheDay.Where(ses => ses.StartTime.Date.Equals(day.StartTime)).ToArray();
                var assetIds = sessionsStartingTheDay.Select(s => s.AssetId).Distinct().ToArray();
                Console.WriteLine("{2}: {0} Sessions started, {1} assets", sessionsStartingTheDay.Length,
                                  assetIds.Length, dayString);

                var analysisOutputFolder = Path.Combine(outputRootFolder,
                                                        dayString);
                Directory.CreateDirectory(analysisOutputFolder);
                AnalyseSessions(sessionsTouchingTheDay, day, analysisOutputFolder);
                foreach (var assetId in assetIds)
                {
                    AnalyseSessionsForAssetId(sessionsTouchingTheDay, day, assetId,
                                              analysisOutputFolder);
                }
            }
        }

        private static Period GetFullPeriod(Session[] completedSessions)
        {
            var endTimes = completedSessions.Select(ses => ses.EndTime).OrderBy(time => time).ToArray();
            return new Period
                {
                    StartTime = endTimes.First(),
                    EndTime = endTimes.Last().Subtract(MaxCorrectDuration),
                };
        }

        private static Period[] GetFullDays(Session[] completedSessions)
        {
            var interestingTimes =
                completedSessions.SelectMany(ses => new[] {ses.StartTime, ses.EndTime});
            var startOfDays = interestingTimes.Select(time => time.Date).Distinct().ToArray();
            return startOfDays.Skip(1).Take(startOfDays.Length - 2).Select(Period.FromDayLocal).ToArray();
        }

        private static void AnalyseSessions(IEnumerable<Session> completedSessions, Period day,
                                            string outputFolder)
        {
            var fileName = Path.Combine(outputFolder, "SessionAnalysis.txt");
            File.WriteAllLines(fileName, AnalyseSessions(completedSessions, day));
        }

        private static void AnalyseSessionsForAssetId(IEnumerable<Session> completedSessions, Period day, string assetId,
                                                      string outputFolder)
        {
            var fileName = Path.Combine(outputFolder, string.Format("SessionAnalysis_{0}.txt", assetId));
            File.WriteAllLines(fileName, AnalyseSessionsForAssetId(completedSessions, day, assetId));
        }

        private class FastSessions
        {
            private readonly TimeSpan m_MaxDuration;
            public FastSessions(Session[] sessions, TimeSpan growAmount)
            {
                m_MaxDuration = MaxCorrectDuration + growAmount;
                m_SessionsByStartTime =
                    sessions.Select(s => new Tuple<DateTime, Session>(s.StartTime, s))
                            .OrderByDescending(t => t.Item1)
                            .ToArray();
            }

            private readonly Tuple<DateTime, Session>[] m_SessionsByStartTime;

            public IEnumerable<Session> SessionsAtTimeX(DateTime x)
            {
                int locateStart = LocateStart(x);
                int locateEarliestStart = LocateStart(x.Subtract(m_MaxDuration));
                int maxCount = locateEarliestStart - locateStart;
                var candidates = m_SessionsByStartTime.Skip(locateStart).Take(maxCount);
                return candidates.Select(t => t.Item2).Where(ses => ses.EndTime > x);
            }

            private int LocateStart(DateTime dt)
            {
                var set = m_SessionsByStartTime;
                int min = 0;
                int max = set.Length - 1;

                while (min < max)
                {
                    int index = (min + max)/2;
                    if (set[index].Item1 > dt)
                        min = index + 1;
                    else
                        max = index;
                }
                return set[max].Item1 > dt ? max + 1 : max;
            }
        }

        private static IEnumerable<DownloadMoment> FindOrReadDownloadTimes(IEnumerable<Session> sessions,
                                                                                          TimeSpan keepAliveTime, string path)
        {
            var fileName = string.Format(@"DownloadTimes_{0}.xml", (int) keepAliveTime.TotalMinutes);
            var pathName = Path.Combine(path, fileName);
            if (File.Exists(pathName))
                return XmlHelper<DownloadMoment[]>.FromFile(pathName);
            var retVal = FindDownloadTimes(sessions, keepAliveTime).ToArray();
            XmlHelper<DownloadMoment[]>.ToFile(retVal, pathName);
            return retVal;
        }

        private static IEnumerable<DownloadMoment> FindDownloadTimes(IEnumerable<Session> sessions,
                                                                                    TimeSpan keepAliveTime)
        {
            var retVal = new List<DownloadMoment>(100000);
            var mySessions = sessions.Select(ses => GrowSessionWithKeepAlive(ses, keepAliveTime)).ToArray();
            var fastSessions = new FastSessions(mySessions, keepAliveTime);
            var interestingTimes =
                mySessions.Select(ses => ses.StartTime).Distinct().OrderBy(time => time).ToArray();
            var lastStartTime = interestingTimes.Last();
            // Only process 3 days
            var firstTime = lastStartTime.AddDays(-3);
            interestingTimes = interestingTimes.SkipWhile(t => t <= firstTime).ToArray();

            var assetIdsInMemory = GetAssetsAtThatTime(fastSessions, firstTime);
            Console.WriteLine("{0} interesting times, first of which is {1}", interestingTimes.Length, firstTime);
            foreach (var time in interestingTimes)
            {
                var assetsAtThatTime = GetAssetsAtThatTime(fastSessions, time);
                //var removedAssets = assetIdsInMemory.Except(assetsAtThatTime).ToArray();
                var downloadedAssets = assetsAtThatTime.Except(assetIdsInMemory).ToArray();
                //Console.WriteLine("{0} ses", downloadedAssets.Length);
                var totalMemoryInUse = Asset.TotalFilesize(assetsAtThatTime);
                assetIdsInMemory = assetsAtThatTime;

                retVal.AddRange(
                    downloadedAssets.Select(
                        assetId => new DownloadMoment(time, Asset.Dictionary[assetId], totalMemoryInUse)));
            }
            Console.WriteLine("{0} downloads in {1}. Max memuse {2}", retVal.Count, retVal.Last().DownloadTime - retVal.First().DownloadTime, retVal.Max(_=>_.TotalMemoryInUse)/(1024*1024*1024));
            return retVal;
        }

        private static HashSet<string> GetAssetsAtThatTime(FastSessions fastSessions, DateTime time)
        {
            var sessionsAtThatTime = fastSessions.SessionsAtTimeX(time).ToArray();
            //mySessions.Where(ses => ses.StartTime <= time && ses.EndTime > time).ToArray();
            //Console.WriteLine("{0} ses", sessionsAtThatTime.Length);
            var assetsAtThatTime = new HashSet<string>(sessionsAtThatTime.Select(s => s.AssetId));
            return assetsAtThatTime;
        }

        private static bool IsLiveIngest(DownloadMoment tup)
        {
            var asset = tup.Asset;
            if (!asset.RestartTv)
                return false;
            var recStart = asset.RecordingStart.Value.AddMinutes(-5);
            var recEnd = asset.RecordingStart.Value.AddSeconds(asset.Duration);
            return tup.DownloadTime >= recStart && tup.DownloadTime < recEnd;
        }

        private static void AnalyseDownloadTimesAndSave(IEnumerable<Session> sessionsEnum, string folder)
        {
            var file = Path.Combine(folder, "MemoryAnalysis.txt");
            File.WriteAllLines(file, AnalyseDownloadTimes(sessionsEnum, folder));
        }

        private static IEnumerable<string> AnalyseDownloadTimes(IEnumerable<Session> sessionsEnum, string cachePath)
        {
            var sessions = sessionsEnum as Session[] ?? sessionsEnum.ToArray();
            for (int minutes = 0; minutes <= 36*60; minutes += 60*3)
            //var minutes = 24*60;
            {
                var keepAliveTime = TimeSpan.FromMinutes(minutes);
                var lines = AnalyseDownloadTimes(sessions, keepAliveTime, cachePath);
                foreach (var line in lines)
                {
                    Console.WriteLine(line);
                    yield return line;
                }
                yield return String.Empty;
            }
        }

        private static IEnumerable<string> AnalyseDownloadTimes(IEnumerable<Session> sessionsEnum,
                                                                TimeSpan keepAliveTime, string cachePath)
        {
            Console.WriteLine("Analysing download times for a keepalive time of {0}", keepAliveTime);
            yield return string.Format("Analysis of download times for a keepalive time of {0}", keepAliveTime);
            var downloadTimes = FindOrReadDownloadTimes(sessionsEnum, keepAliveTime, cachePath).ToArray();
            //downloadTimes = downloadTimes.Where(dt => dt.DownloadTime < DateTime.Now.Date.AddDays(-1)).ToArray();
            var peakMemoryUse = downloadTimes.Max(dt => dt.TotalMemoryInUse);
            yield return string.Format("Peak memory use: {0} Gb", peakMemoryUse/(1024*1024*1024));
            yield return string.Format("{0} downloads or live ingests", downloadTimes.Length);
            downloadTimes = downloadTimes.Where(tup => !IsLiveIngest(tup)).ToArray();
            yield return string.Format("{0} downloads", downloadTimes.Length);

            var firstTime = downloadTimes.First().DownloadTime;
            var lastTime = downloadTimes.Last().DownloadTime;
            foreach (var p in AnalyseDownloadsPerPeriod(firstTime, lastTime, TimeSpan.FromMinutes(1), downloadTimes)) yield return p;
            foreach (var p in AnalyseDownloadsPerPeriod(firstTime, lastTime, TimeSpan.FromHours(1), downloadTimes)) yield return p;
            var bitrateDumpFile = Path.Combine(cachePath,
                                               string.Format("ConcurrentBitrates_{0}.txt",
                                                             (int) keepAliveTime.TotalMinutes));
            foreach (var p in AnalyseDownloadBitrates(firstTime, downloadTimes, bitrateDumpFile)) yield return p;
        }

        private static IEnumerable<string> AnalyseDownloadBitrates(DateTime firstTime, DownloadMoment[] downloadTimes, string csvFilePath)
        {
            var interestingTimes =
                downloadTimes.SelectMany(dt => new[] {dt.DownloadTime, dt.EndTime, dt.DownloadTime.AddSeconds(-1), dt.EndTime.AddSeconds(1)})
                             .Where(t => t >= firstTime).Distinct().OrderBy(_ => _).ToArray();
            var concurrentDownloadsOverTime =
                interestingTimes.Select(t => new Tuple<DateTime, int>(t, ConcurrentDownloads(t, downloadTimes))).ToArray();
            var max = concurrentDownloadsOverTime.OrderByDescending(_ => _.Item2).First();
            yield return
                string.Format("Max concurrent downloads: {0} ({1} Mbps) at {2}", max.Item2,
                              max.Item2*Asset.DownloadBitrate/(1024*1024), max.Item1);
            DateTime excelRefDate = DateTime.Parse("1900-01-01T00:00:00").AddDays(-2);
            var csvLines = new[] { "ExcelTime\tTime\tDownloads\tBitrate" }.Concat(concurrentDownloadsOverTime.Select(t => string.Format("{0}\t{3}\t{1}\t{2}", (t.Item1-excelRefDate).TotalDays, t.Item2, t.Item2 * Asset.DownloadBitrate / (1024 * 1024), t.Item1.ToString())));
            File.WriteAllLines(csvFilePath,csvLines);
        }

        private static int ConcurrentDownloads(DateTime time, DownloadMoment[] downloadTimes)
        {
            var downloadsAtTime = downloadTimes.Where(dt => dt.DownloadTime <= time && dt.EndTime > time).ToArray();
            return downloadsAtTime.Length;
        }

        private static IEnumerable<string> AnalyseDownloadsPerPeriod(DateTime firstTime, DateTime lastTime, TimeSpan periodLength,
                                                             DownloadMoment[] downloadTimes)
        {
            var maxBytesPerPeriod = 0L;
            int maxDownloadsPerPeriod = 0;
            for (var periodStart = firstTime; periodStart < lastTime; periodStart = periodStart.Add(periodLength))
            {
                var period = new Period {StartTime = periodStart, EndTime = periodStart + periodLength};
                var downloadsInPeriod = downloadTimes.Where(dt => period.Contains(dt.DownloadTime)).ToArray();
                var bytesThisPeriod = downloadsInPeriod.Sum(dt => dt.Asset.Filesize);
                maxBytesPerPeriod = Math.Max(maxBytesPerPeriod, bytesThisPeriod);
                maxDownloadsPerPeriod = Math.Max(maxDownloadsPerPeriod, downloadsInPeriod.Length);
            }
            yield return string.Format("Max downloads in period of {0}: {1}", periodLength, maxDownloadsPerPeriod);
            yield return string.Format("Max downloaded Mb in period of {0}: {1}", periodLength, maxBytesPerPeriod/(1024*1024));
        }

        private static Session GrowSessionWithKeepAlive(Session ses, TimeSpan keepAliveTime)
        {
            return new Session
                {
                    AssetId = ses.AssetId,
                    StartTime = ses.StartTime,
                    EndTime = ses.EndTime + keepAliveTime,
                };
        }

        private static IEnumerable<string> AnalyseSessions(IEnumerable<Session> completedSessions, Period day)
        {
            var mySessions = completedSessions.ToArray();
            var fastSessions = new FastSessions(mySessions, default(TimeSpan));

            yield return string.Format("Period: {0}-{1}", day.StartTime, day.EndTime);
            yield return string.Format("{0} Sessions", mySessions.Length);
            var assetIds = mySessions.Select(ses => ses.AssetId).Distinct().ToArray();
            yield return
                string.Format("{0} Assets, total {1} Gb", assetIds.Length,
                              Asset.TotalFilesize(assetIds)/(1024*1024*1024));

            var interestingTimes =
                mySessions.SelectMany(ses => new[] {ses.StartTime, ses.EndTime})
                          .Where(day.Contains)
                          .Distinct()
                          .OrderBy(time => time);

            foreach (var time in interestingTimes)
            {
                var sessionsAtThatTime = fastSessions.SessionsAtTimeX(time).ToArray();
                var assetsAtThatTime = sessionsAtThatTime.Select(s => s.AssetId).Distinct().ToArray();
                var sessionCount = sessionsAtThatTime.Count();
                var assetCount = assetsAtThatTime.Count();
                var assetSize = Asset.TotalFilesize(assetsAtThatTime)/(1024*1024*1024);

                yield return
                    string.Format("At {0}, {1} sessions for {2} assets ({3} Gb)", time.ToLocalTime(), sessionCount,
                                  assetCount, assetSize);
                if (sessionCount == 0)
                    yield return string.Empty;
            }
        }

        private static IEnumerable<string> AnalyseSessionsForAssetId(IEnumerable<Session> completedSessions, Period day,
                                                                     string assetId)
        {
            var retVal = new List<string>();
            var mySessions =
                completedSessions.Where(ses => ses.AssetId.Equals(assetId, StringComparison.InvariantCultureIgnoreCase))
                                 .ToArray();
            var fastSessions = new FastSessions(mySessions, default(TimeSpan));

            retVal.Add(string.Format("Asset: {0}", assetId));
            retVal.Add(string.Format("{0} Sessions", mySessions.Length));

            var interestingTimes =
                mySessions.SelectMany(ses => new[] {ses.StartTime, ses.EndTime})
                          .Where(day.Contains)
                          .Distinct()
                          .OrderBy(time => time);

            foreach (var time in interestingTimes)
            {
                var sessionsAtThatTime = fastSessions.SessionsAtTimeX(time).ToArray();
                var count = sessionsAtThatTime.Count();
                retVal.Add(string.Format("At {0}, {1} sessions", time.ToLocalTime(), count));
                if (count == 0)
                    retVal.Add(string.Empty);
            }
            return retVal;
        }

        private static IEnumerable<Session> GetCompletedSessionsFromVodUsageFolder(string folderName)
        {
            var fastFile = folderName + "_fast.xml";
            if (File.Exists(fastFile))
                return XmlHelper<Session[]>.FromFile(fastFile);
            var retVal = Directory.EnumerateFiles(folderName, "VODUsage*.xml")
                                  .SelectMany(GetCompletedSessionsFromVodUsageFile).ToArray();
            XmlHelper<Session[]>.ToFile(retVal, fastFile);
            return retVal;
        }

        private static readonly XNamespace Ns = "urn:eventis:vodusage:2.0";

        private static readonly TimeSpan MaxCorrectDuration = TimeSpan.FromHours(6);

        private static IEnumerable<Session> GetCompletedSessionsFromVodUsageFile(string fileName)
        {
            var doc = XDocument.Load(fileName);
            var fsElements = doc.Root.Elements(Ns + "FinishedSessions").Elements(Ns + "FinishedSession");
            return fsElements.Select(SessionForFinishedSessionElement).Where(s => s.Duration < MaxCorrectDuration);
        }

        private static Session SessionForFinishedSessionElement(XElement fs)
        {
            var assetIdElement = fs.Elements(Ns + "AssetId").Single();
            var periodElement = fs.Elements(Ns + "SessionPeriod").Single();
            return new Session
                {
                    AssetId = assetIdElement.Value,
                    StartTime =
                        DateTime.Parse(periodElement.Attributes("startDate").Single().Value, null,
                                       DateTimeStyles.RoundtripKind).ToLocalTime(),
                    EndTime =
                        DateTime.Parse(periodElement.Attributes("endDate").Single().Value, null,
                                       DateTimeStyles.RoundtripKind).ToLocalTime(),
                };
        }
    }
}

