// be_wrk_orchestrator/Program.cs
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

// --- USING DIRECTIVES ---
using be_wrk_orchestrator; // To find OrchestratorWorker
using core_lib_messaging.RabbitMq; // To find AddRabbitMqService extension method
// --- END USING DIRECTIVES ---

namespace be_wrk_orchestrator
{
    public class Program
    {
        public static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureLogging(logging =>
                {
                    logging.ClearProviders();
                    logging.AddConsole();
                    logging.AddDebug(); // Optional: Add debug output if needed, useful for orchestration logging
                })
                .ConfigureServices((hostContext, services) =>
                {
                    // Register our custom RabbitMQ service
                    services.AddRabbitMqService();

                    // --- Add Redis Connection --- //
                    services.AddSingleton<IConnectionMultiplexer>(sp => 
                        ConnectionMultiplexer.Connect(hostContext.Configuration.GetConnectionString("Redis")));

                    // Register our OrchestratorWorker as a hosted service
                    services.AddHostedService<OrchestratorWorker>();
                });
    }
}