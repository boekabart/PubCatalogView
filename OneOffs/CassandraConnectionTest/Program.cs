using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;

namespace CassandraConnectionTest
{
    enum RetVal
    {
        OK,
        Timeout,
        Error,
    }

    class Program
    {
        static void Main(string[] args)
        {
            var logFileName = string.Format("CassConnTest_{0}.log", args[0]);
            var errorLogFileName = string.Format("CassConnTest_{0}_Errors.log", args[0]);
            var previous = RetVal.OK;
            while (!Console.KeyAvailable)
            {
                var retval = TryConnect(args[0]);
                var logline = string.Format("{0:yyy-MM-dd HH:mm:ss}: {1}", DateTime.Now, retval);
                if (retval != RetVal.OK || retval != previous)
                    Console.WriteLine(String.Empty);
                Console.Write(logline + "\r");

                if (retval != RetVal.OK)
                    File.AppendAllLines(errorLogFileName, new[] {logline});

                File.AppendAllLines(logFileName, new[] {logline});
                System.Threading.Thread.Sleep(1000);
                previous = retval;
            }
        }

        private static RetVal TryConnect(string host)
        {
            using (var tcp = new TcpClient())
            {
                var ar = tcp.BeginConnect(host, 9160, null, null);
                var wh = ar.AsyncWaitHandle;
                try
                {
                    if (!ar.AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(5), false))
                    {
                        tcp.Close();
                        return RetVal.Timeout;
                    }

                    tcp.EndConnect(ar);
                    return RetVal.OK;
                }
                catch (Exception e)
                {
                    Console.WriteLine(String.Empty);
                    Console.Error.Write(e.Message);
                    return RetVal.Error;
                }
                finally
                {
                    wh.Close();
                }
            }
        }
    }
}
