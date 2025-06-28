// be_wrk_orchestrator/OrchestratorWorker.cs
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent; // For ConcurrentDictionary
using System.Threading;
using System.Threading.Tasks;

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
        // Using ConcurrentDictionary for in-memory state management.
        // The key for this dictionary MUST be string, so CorrelationId (Guid) must be converted to string.
        private readonly ConcurrentDictionary<string, OrchestrationState> _orchestrationStates;

        public OrchestratorWorker(ILogger<OrchestratorWorker> logger, IRabbitMqService rabbitMqService)
        {
            _logger = logger;
            _rabbitMqService = rabbitMqService;
            _orchestrationStates = new ConcurrentDictionary<string, OrchestrationState>();
        }

        // Represents the state of an ongoing orchestration workflow
        private class OrchestrationState
        {
            public FeederResponse? FeederResponse { get; set; }
            public DateTime StartTime { get; } = DateTime.UtcNow;
            public string CurrentStep { get; set; } = "Initialized";
            public bool IsCompleted { get; set; } = false;
            public bool IsSuccessful { get; set; } = false;
            public string? ErrorMessage { get; set; }
            // As discussed, OrchestratorResponse publishing needs a ReplyToQueue,
            // which would ideally come from an initial trigger message.
            // Since Orchestrator initiates, there's no external ReplyToQueue to use unless hardcoded.
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker starting...");

            try
            {
                // Declare queues for services it interacts with (Feeder, Writer)
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ReqFeederQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ResFeederQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ReqWriterQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ResWriterQueue);

                // Purge all queues before starting orchestration
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ReqFeederQueue);
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ResFeederQueue);
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ReqWriterQueue);
                _rabbitMqService.PurgeQueue(RabbitMqConfig.ResWriterQueue);

                // Set up Consumers:
                _rabbitMqService.Consume<FeederResponse>(RabbitMqConfig.ResFeederQueue, OnFeederResponseReceived, autoAck: false);
                _rabbitMqService.Consume<WriterResponse>(RabbitMqConfig.ResWriterQueue, OnWriterResponseReceived, autoAck: false);

                _logger.LogInformation($"be_wrk_orchestrator: Listening for feeder responses on '{RabbitMqConfig.ResFeederQueue}'");
                _logger.LogInformation($"be_wrk_orchestrator: Listening for writer responses on '{RabbitMqConfig.ResWriterQueue}'");

                // Initiate the workflow by sending FeederCommand on startup ---
                var initialCorrelationId = Guid.NewGuid();
                var initialFeederCommand = new FeederCommand
                {
                    CorrelationId = initialCorrelationId,
                    CommandType = $"StartupProcess_{DateTime.Now:yyyyMMddHHmmss}", // A dynamic type for the initial command
                    // You can add more initial data here if needed, e.g., RequestData = "some_data_from_startup_config"
                };

                // Create the initial OrchestrationState before sending the command
                var initialState = new OrchestrationState
                {
                    // If you want to store the command that kicked it off: InitialCommand = initialFeederCommand,
                    CurrentStep = "Orchestrator Initiating Feeder Command"
                };
                _orchestrationStates.TryAdd(initialCorrelationId.ToString(), initialState);

                // Publish the initial FeederCommand asynchronously.
                // Using .ContinueWith to log success/failure of this initial publish attempt.
                _ = _rabbitMqService.PublishAsync(RabbitMqConfig.ReqFeederQueue, initialFeederCommand)
                    .ContinueWith(task => {
                        if (task.IsFaulted)
                        {
                            _logger.LogError(task.Exception, $"be_wrk_orchestrator: [!] Error publishing initial FeederCommand for CorrelationId: {initialCorrelationId}");
                            // Optionally, update the state to failed if the initial command couldn't be sent
                            if (_orchestrationStates.TryGetValue(initialCorrelationId.ToString(), out var failedState))
                            {
                                failedState.IsCompleted = true;
                                failedState.IsSuccessful = false;
                                failedState.ErrorMessage = "Failed to publish initial FeederCommand";
                            }
                        }
                        else
                        {
                            _logger.LogInformation($"be_wrk_orchestrator: [->] Initial FeederCommand with CorrelationId: {initialCorrelationId} published to '{RabbitMqConfig.ReqFeederQueue}' on startup.");
                        }
                    });
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
                // Orchestrator primarily reacts to messages, so the ExecuteAsync
                // loop can simply delay, waiting for cancellation.
                await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken);

                // Periodically log current ongoing orchestrations (optional for debugging)
                if (_orchestrationStates.Count > 0)
                {
                    _logger.LogDebug($"be_wrk_orchestrator: Currently tracking {_orchestrationStates.Count} ongoing orchestrations.");
                }
            }
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker detected cancellation request.");
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker stopping...");
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker stopped gracefully.");
            return base.StopAsync(cancellationToken);
        }

        /// <summary>
        /// Handler for responses from the Feeder Service.
        /// This method will now always receive a response for an orchestration initiated by the orchestrator itself.
        /// </summary>
        private async Task OnFeederResponseReceived(FeederResponse? response, MessageDeliveryContext context)
        {
            // Ensure response and CorrelationId are valid. CorrelationId in BaseMessage is a Guid.
            if (response == null || response.CorrelationId == Guid.Empty)
            {
                _logger.LogWarning("be_wrk_orchestrator: [!] Error: Received null or malformed FeederResponse (invalid CorrelationId). Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }

            // Convert CorrelationId (Guid) to string for dictionary lookup and logging
            string correlationIdString = response.CorrelationId.ToString();
            OrchestrationState? state;

            // The state *should* now always exist because the orchestrator initiated it in StartAsync.
            // If it doesn't, it indicates an unexpected message or an issue.
            if (!_orchestrationStates.TryGetValue(correlationIdString, out state))
            {
                _logger.LogWarning($"be_wrk_orchestrator: [!] Received FeederResponse for unknown CorrelationId: {correlationIdString}. This should only happen if another service initiated it or after completion. Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }

            _logger.LogInformation($"be_wrk_orchestrator: [<-] Received FeederResponse for CorrelationId: {correlationIdString}. IsSuccess: {response.IsSuccess}. Current Step: {state.CurrentStep}");

            state.FeederResponse = response; // Store feeder response in state
            state.CurrentStep = "Processing Feeder Response";

            try
            {
                if (response.IsSuccess)
                {
                    if (string.IsNullOrEmpty(response.FetchedData))
                    {
                        _logger.LogWarning($"be_wrk_orchestrator: [!] Feeder returned success but no data for CorrelationId: {correlationIdString}. Proceeding with empty data to writer.");
                        // In a real scenario, you might have specific logic for empty data (e.g., skip writer, fail orchestration).
                    }

                    // Send command to Writer
                    var writerCommand = new WriterCommand
                    {
                        CorrelationId = response.CorrelationId, // CorrelationId on WriterCommand should match the Guid type
                        DataToPersist = response.FetchedData // Pass fetched data to writer
                    };

                    state.CurrentStep = "Requesting Writer Data";
                    await _rabbitMqService.PublishAsync(RabbitMqConfig.ReqWriterQueue, writerCommand);
                    _rabbitMqService.Ack(context.DeliveryTag); // Acknowledge the feeder response
                    _logger.LogInformation($"be_wrk_orchestrator: [->] Published WriterCommand for CorrelationId: {correlationIdString}");
                }
                else
                {
                    _logger.LogWarning($"be_wrk_orchestrator: [!] Feeder step failed for CorrelationId: {correlationIdString}. Error: {response.ErrorMessage}");
                    HandleOrchestrationFailure(correlationIdString, $"Feeder step failed: {response.ErrorMessage}", context.DeliveryTag, requeueOriginal: false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"be_wrk_orchestrator: [!] Error processing FeederResponse or publishing WriterCommand for CorrelationId: {correlationIdString}.");
                HandleOrchestrationFailure(correlationIdString, $"Orchestrator internal error after Feeder response: {ex.Message}", context.DeliveryTag, requeueOriginal: false);
            }
        }

        /// <summary>
        /// Handler for responses from the Writer Service. Finalizes the workflow.
        /// </summary>
        private async Task OnWriterResponseReceived(WriterResponse? response, MessageDeliveryContext context)
        {
            // Ensure response and CorrelationId are valid. CorrelationId in BaseMessage is a Guid.
            if (response == null || response.CorrelationId == Guid.Empty)
            {
                _logger.LogWarning("be_wrk_orchestrator: [!] Error: Received null or malformed WriterResponse (invalid CorrelationId). Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }

            // Convert CorrelationId (Guid) to string for dictionary lookup and logging
            string correlationIdString = response.CorrelationId.ToString();

            // Try to remove the state as this is the final step in this orchestration
            if (!_orchestrationStates.TryRemove(correlationIdString, out var state))
            {
                _logger.LogWarning($"be_wrk_orchestrator: [!] Received WriterResponse for unknown or already completed CorrelationId: {correlationIdString}. Nacking.");
                _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                return;
            }

            _logger.LogInformation($"be_wrk_orchestrator: [<-] Received WriterResponse for CorrelationId: {correlationIdString}. IsSuccess: {response.IsSuccess}");

            // Finalize orchestration state
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
                // As discussed, the OrchestratorResponse is not published to a dedicated queue
                // and lacks a ReplyToQueue property in BaseMessage to send dynamically.
                // It's currently not being sent externally.
                _logger.LogInformation($"be_wrk_orchestrator: [->] Orchestration process completed for CorrelationId: {correlationIdString}. Final response not published to a dedicated orchestrator queue as per design.");
                _rabbitMqService.Ack(context.DeliveryTag); // Acknowledge writer response message

                // [NEW] Start the process again after writer finishes
                var newCorrelationId = Guid.NewGuid();
                var newFeederCommand = new FeederCommand
                {
                    CorrelationId = newCorrelationId,
                    CommandType = $"ChainedProcess_{DateTime.Now:yyyyMMddHHmmss}",
                    // Optionally, add more data here if needed
                };
                var newState = new OrchestrationState
                {
                    CurrentStep = "Orchestrator Initiating Feeder Command"
                };
                _orchestrationStates.TryAdd(newCorrelationId.ToString(), newState);

                await _rabbitMqService.PublishAsync(RabbitMqConfig.ReqFeederQueue, newFeederCommand);
                _logger.LogInformation($"be_wrk_orchestrator: [->] Chained FeederCommand with CorrelationId: {newCorrelationId} published to '{RabbitMqConfig.ReqFeederQueue}' after Writer completed.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"be_wrk_orchestrator: [!] Error processing or finalizing Orchestration for CorrelationId: {correlationIdString}.");
            }
        }

        /// <summary>
        /// Helper method to handle failures in the orchestration flow and publish a final failure response.
        /// </summary>
        /// <param name="correlationId">The correlation ID as a string (since it's used as a dictionary key).</param>
        private async void HandleOrchestrationFailure(string correlationId, string errorMessage, ulong deliveryTag, bool requeueOriginal)
        {
            // Try to remove the state if it exists. The 'correlationId' parameter here is already a string.
            if (_orchestrationStates.TryRemove(correlationId, out var state))
            {
                state.IsCompleted = true;
                state.IsSuccessful = false;
                state.ErrorMessage = errorMessage;
                state.CurrentStep = "Failed";
                _logger.LogError($"be_wrk_orchestrator: Orchestration FAILED for CorrelationId: {correlationId}. Reason: {errorMessage}");

                // Nack the current message that triggered the failure (e.g., failed FeederResponse)
                _rabbitMqService.Nack(deliveryTag, requeueOriginal);

                // Publish a final failure response (currently not sent externally as per design constraints)
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
            else
            {
                _logger.LogWarning($"be_wrk_orchestrator: Tried to handle failure for unknown or already processed CorrelationId: {correlationId}. Nacking current message.");
                _rabbitMqService.Nack(deliveryTag, requeueOriginal); // Still nack the message even if state not found
            }
        }
    }
}