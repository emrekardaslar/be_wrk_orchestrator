
// be_wrk_orchestrator/OrchestratorWorker.cs
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

// --- USING DIRECTIVES FOR core_lib_messaging ---
using core_lib_messaging.Models;
using core_lib_messaging.RabbitMq;
// --- END USING DIRECTIVES ---

namespace be_wrk_orchestrator
{
    public class OrchestratorWorker : BackgroundService
    {
        private readonly ILogger<OrchestratorWorker> _logger;
        private readonly IRabbitMqService _rabbitMqService;
        private readonly IDatabase _redisDatabase;

        public OrchestratorWorker(ILogger<OrchestratorWorker> logger, IRabbitMqService rabbitMqService, IConnectionMultiplexer redisConnection)
        {
            _logger = logger;
            _rabbitMqService = rabbitMqService;
            _redisDatabase = redisConnection.GetDatabase();
        }

        // Represents the state of an ongoing orchestration workflow
        private class OrchestrationState
        {
            public FeederResponse? FeederResponse { get; set; }
            public List<string> Ids { get; set; } = new List<string>();
            public DateTime StartTime { get; } = DateTime.UtcNow;
            public string CurrentStep { get; set; } = "Initialized";
            public bool IsCompleted { get; set; } = false;
            public bool IsSuccessful { get; set; } = false;
            public string? ErrorMessage { get; set; }
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker starting...");

            try
            {
                // Declare queues for services it interacts with (Feeder, Fetcher, Writer)
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ReqFeederQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ResFeederQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ReqFetcherQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ResFetcherQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ReqWriterQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ResWriterQueue);

                // Purge all queues before starting orchestration
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ReqFeederQueue);
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ResFeederQueue);
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ReqFetcherQueue);
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ResFetcherQueue);
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ReqWriterQueue);
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ResWriterQueue);

                // Set up Consumers:
                _rabbitMqService.Consume<FeederResponse>(RabbitMqConfig.ResFeederQueue, OnFeederResponseReceived, autoAck: false);
                _rabbitMqService.Consume<FetcherResponse>(RabbitMqConfig.ResFetcherQueue, OnFetcherResponseReceived, autoAck: false);
                _rabbitMqService.Consume<WriterResponse>(RabbitMqConfig.ResWriterQueue, OnWriterResponseReceived, autoAck: false);

                _logger.LogInformation($"be_wrk_orchestrator: Listening for feeder responses on '{RabbitMqConfig.ResFeederQueue}'");
                _logger.LogInformation($"be_wrk_orchestrator: Listening for fetcher responses on '{RabbitMqConfig.ResFetcherQueue}'");
                _logger.LogInformation($"be_wrk_orchestrator: Listening for writer responses on '{RabbitMqConfig.ResWriterQueue}'");

                // Use the helper for the initial orchestration
                _ = StartNewOrchestration($"StartupProcess_{DateTime.Now:yyyyMMddHHmmss}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "be_wrk_orchestrator: Error starting Orchestrator Worker. Shutting down.");
                throw;
            }

            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker running. Press Ctrl+C to stop.");
            while (!stoppingToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);
            }
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker detected cancellation request.");
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker stopping...");
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker stopped gracefully.");
            return base.StopAsync(cancellationToken);
        }

        private async Task OnFeederResponseReceived(FeederResponse? response, MessageDeliveryContext context)
        {
            if (response == null || response.CorrelationId == Guid.Empty)
            {
                _logger.LogWarning("be_wrk_orchestrator: [!] Error: Received null or malformed FeederResponse (invalid CorrelationId). Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }

            string correlationIdString = response.CorrelationId.ToString();
            var stateJson = await _redisDatabase.StringGetAsync(correlationIdString);
            if (stateJson.IsNullOrEmpty)
            {
                _logger.LogWarning($"be_wrk_orchestrator: [!] Received FeederResponse for unknown CorrelationId: {correlationIdString}. Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }

            var state = JsonSerializer.Deserialize<OrchestrationState>(stateJson);
            _logger.LogInformation($"be_wrk_orchestrator: [<-] Received FeederResponse for CorrelationId: {correlationIdString}. IsSuccess: {response.IsSuccess}. Current Step: {state.CurrentStep}");

            state.FeederResponse = response;
            state.Ids = response.Ids;
            state.CurrentStep = "Processing Feeder Response";

            try
            {
                if (response.IsSuccess)
                {
                    var fetcherCommand = new FetcherCommand
                    {
                        CorrelationId = response.CorrelationId,
                        Ids = response.Ids
                    };

                    state.CurrentStep = "Requesting Fetcher Data";
                    await _redisDatabase.StringSetAsync(correlationIdString, JsonSerializer.Serialize(state));
                    await _rabbitMqService.PublishAsync(RabbitMqConfig.ReqFetcherQueue, fetcherCommand);
                    _rabbitMqService.Ack(context.DeliveryTag);
                    _logger.LogInformation($"be_wrk_orchestrator: [->] Published FetcherCommand for CorrelationId: {correlationIdString}");
                }
                else
                {
                    _logger.LogWarning($"be_wrk_orchestrator: [!] Feeder step failed for CorrelationId: {correlationIdString}. Error: {response.ErrorMessage}");
                    await HandleOrchestrationFailure(correlationIdString, $"Feeder step failed: {response.ErrorMessage}", context.DeliveryTag, requeueOriginal: false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"be_wrk_orchestrator: [!] Error processing FeederResponse or publishing FetcherCommand for CorrelationId: {correlationIdString}.");
                await HandleOrchestrationFailure(correlationIdString, $"Orchestrator internal error after Feeder response: {ex.Message}", context.DeliveryTag, requeueOriginal: false);
            }
        }

        private async Task OnFetcherResponseReceived(FetcherResponse? response, MessageDeliveryContext context)
        {
            if (response == null || response.CorrelationId == Guid.Empty)
            {
                _logger.LogWarning("be_wrk_orchestrator: [!] Error: Received null or malformed FetcherResponse (invalid CorrelationId). Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }

            string correlationIdString = response.CorrelationId.ToString();
            var stateJson = await _redisDatabase.StringGetAsync(correlationIdString);
            if (stateJson.IsNullOrEmpty)
            {
                _logger.LogWarning($"be_wrk_orchestrator: [!] Received FetcherResponse for unknown CorrelationId: {correlationIdString}. Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }

            var state = JsonSerializer.Deserialize<OrchestrationState>(stateJson);
            _logger.LogInformation($"be_wrk_orchestrator: [<-] Received FetcherResponse for CorrelationId: {correlationIdString}. IsSuccess: {response.IsSuccess}. Current Step: {state.CurrentStep}");

            state.CurrentStep = "Processing Fetcher Response";

            try
            {
                if (response.IsSuccess)
                {
                    var writerCommand = new WriterCommand
                    {
                        CorrelationId = response.CorrelationId
                    };

                    state.CurrentStep = "Requesting Writer Data";
                    await _redisDatabase.StringSetAsync(correlationIdString, JsonSerializer.Serialize(state));
                    await _rabbitMqService.PublishAsync(RabbitMqConfig.ReqWriterQueue, writerCommand);
                    _rabbitMqService.Ack(context.DeliveryTag);
                    _logger.LogInformation($"be_wrk_orchestrator: [->] Published WriterCommand for CorrelationId: {correlationIdString}");
                }
                else
                {
                    _logger.LogWarning($"be_wrk_orchestrator: [!] Fetcher step failed for CorrelationId: {correlationIdString}. Error: {response.ErrorMessage}");
                    await HandleOrchestrationFailure(correlationIdString, $"Fetcher step failed: {response.ErrorMessage}", context.DeliveryTag, requeueOriginal: false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"be_wrk_orchestrator: [!] Error processing FetcherResponse or publishing WriterCommand for CorrelationId: {correlationIdString}.");
                await HandleOrchestrationFailure(correlationIdString, $"Orchestrator internal error after Fetcher response: {ex.Message}", context.DeliveryTag, requeueOriginal: false);
            }
        }

        private async Task OnWriterResponseReceived(WriterResponse? response, MessageDeliveryContext context)
        {
            if (response == null || response.CorrelationId == Guid.Empty)
            {
                _logger.LogWarning("be_wrk_orchestrator: [!] Error: Received null or malformed WriterResponse (invalid CorrelationId). Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }

            string correlationIdString = response.CorrelationId.ToString();
            var stateJson = await _redisDatabase.StringGetAsync(correlationIdString);
            if (stateJson.IsNullOrEmpty)
            {
                 _logger.LogWarning($"be_wrk_orchestrator: [!] Received WriterResponse for unknown or already completed CorrelationId: {correlationIdString}. Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }
            await _redisDatabase.KeyDeleteAsync(correlationIdString);

            var state = JsonSerializer.Deserialize<OrchestrationState>(stateJson);
            _logger.LogInformation($"be_wrk_orchestrator: [<-] Received WriterResponse for CorrelationId: {correlationIdString}. IsSuccess: {response.IsSuccess}");

            state.IsCompleted = true;
            state.CurrentStep = "Completed";

            var orchestrationResponse = new OrchestratorResponse
            {
                CorrelationId = response.CorrelationId,
                IsSuccess = response.IsSuccess,
                FinalMessage = response.IsSuccess ? "Orchestration completed successfully." : "Orchestration failed at writer step.",
                PersistedItemId = response.PersistedItemId,
                ErrorDetails = response.ErrorMessage
            };

            if (response.IsSuccess)
            {
                state.IsSuccessful = true;
                _logger.LogInformation($"be_wrk_orchestrator: [V] Orchestration SUCCESS for CorrelationId: {correlationIdString}. Persisted ID: {response.PersistedItemId ?? "N/A"}");
            }
            else
            {
                state.IsSuccessful = false;
                state.ErrorMessage = $"Writer step failed: {response.ErrorMessage}";
                _logger.LogWarning($"be_wrk_orchestrator: [X] Orchestration FAILED for CorrelationId: {correlationIdString}. Reason: {response.ErrorMessage}");
            }

            try
            {
                _logger.LogInformation($"be_wrk_orchestrator: [->] Orchestration process completed for CorrelationId: {correlationIdString}. Final response not published to a dedicated orchestrator queue as per design.");
                _rabbitMqService.Ack(context.DeliveryTag);

                await StartNewOrchestration($"ChainedProcess_{DateTime.Now:yyyyMMddHHmmss}");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"be_wrk_orchestrator: [!] Error processing or finalizing Orchestration for CorrelationId: {correlationIdString}.");
            }
        }

        private async Task HandleOrchestrationFailure(string correlationId, string errorMessage, ulong deliveryTag, bool requeueOriginal)
        {
            await _redisDatabase.KeyDeleteAsync(correlationId);
            _logger.LogError($"be_wrk_orchestrator: Orchestration FAILED for CorrelationId: {correlationId}. Reason: {errorMessage}");

            _rabbitMqService.Nack(deliveryTag, requeueOriginal);

            var orchestrationResponse = new OrchestratorResponse
            {
                CorrelationId = Guid.Parse(correlationId),
                IsSuccess = false,
                FinalMessage = "Orchestration failed due to an internal error or a step failure.",
                ErrorDetails = errorMessage
            };

            try
            {
                _logger.LogInformation($"be_wrk_orchestrator: [->] Orchestration FAILED for CorrelationId: {correlationId}. Final error response not published to a dedicated orchestrator queue as per design.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"be_wrk_orchestrator: [!] Error processing or finalizing FAILED Orchestration for CorrelationId: {correlationId}.");
            }
        }

        private async Task StartNewOrchestration(string commandType)
        {
            var correlationId = Guid.NewGuid();
            var feederCommand = new FeederCommand
            {
                CorrelationId = correlationId,
                CommandType = commandType
            };
            var state = new OrchestrationState
            {
                CurrentStep = "Orchestrator Initiating Feeder Command"
            };
            await _redisDatabase.StringSetAsync(correlationId.ToString(), JsonSerializer.Serialize(state), TimeSpan.FromMinutes(30));
            await _rabbitMqService.PublishAsync(RabbitMqConfig.ReqFeederQueue, feederCommand);
            _logger.LogInformation($"be_wrk_orchestrator: [->] FeederCommand with CorrelationId: {correlationId} published to '{RabbitMqConfig.ReqFeederQueue}'.");
        }
    }
}
