using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Naveego.Pipeline.Hosting;

namespace SharePointPublisher
{
    class Program
    {
        static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Usage: publisher.exe <listener_address>");
                return 1;
            }

            var server = new Host(args[0])
                .WithPublisher(new SharePointPublisher())
                .Run();

            server.Wait();
            return 0;
        }
    }
}
