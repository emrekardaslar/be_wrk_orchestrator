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
            // InitialCommand will now refer to the *triggering* message, which might be a FeederResponse,
            // as OrchestratorCommand is no longer directly consumed by the orchestrator.
            // For now, keeping it as OrchestratorCommand? but note its role has changed conceptually.
            public OrchestratorCommand? InitialCommand { get; set; }
            public FeederResponse? FeederResponse { get; set; }
            public DateTime StartTime { get; } = DateTime.UtcNow;
            public string CurrentStep { get; set; } = "Initialized";
            public bool IsCompleted { get; set; } = false;
            public bool IsSuccessful { get; set; } = false;
            public string? ErrorMessage { get; set; }
            // If the orchestrator is to send a final response, it needs a ReplyTo address.
            // This would ideally come from the *initial trigger message*.
            // public string? ReplyToQueue { get; set; } // This would require a library change if BaseMessage/FeederResponse doesn't have it.
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("be_wrk_orchestrator: Orchestrator Worker starting...");

            try
            {
                // The orchestrator needs to know about these queues to send commands and receive responses.
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ReqFeederQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ResFeederQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ReqWriterQueue);
                _rabbitMqService.DeclareQueueWithDeadLetter(RabbitMqConfig.ResWriterQueue);

                // Consume responses from the Feeder service (this will now be the primary entry/reaction point)
                _rabbitMqService.Consume<FeederResponse>(RabbitMqConfig.ResFeederQueue, OnFeederResponseReceived, autoAck: false);
                // Consume responses from the Writer service
                _rabbitMqService.Consume<WriterResponse>(RabbitMqConfig.ResWriterQueue, OnWriterResponseReceived, autoAck: false);


                _logger.LogInformation($"be_wrk_orchestrator: Listening for feeder responses on '{RabbitMqConfig.ResFeederQueue}'");
                _logger.LogInformation($"be_wrk_orchestrator: Listening for writer responses on '{RabbitMqConfig.ResWriterQueue}'");
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

        // Removed: OnOrchestratorCommandReceived method entirely, as there's no dedicated queue for it.
        // The initial trigger for an orchestration will now need to come from a FeederResponse (or similar).

        /// <summary>
        /// Handler for responses from the Feeder Service. Decides next step (Writer or Fail).
        /// This method might also need to act as the *initial trigger* for new orchestrations
        /// if FeederResponse carries a special "start orchestration" signal.
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

            // --- NEW ORCHESTRATION INITIATION LOGIC (Conceptual) ---
            // If this FeederResponse is meant to *start* a new orchestration,
            // you'd add logic here to check for that condition.
            // Example: if (response.CommandType == "InitiateOrchestration" && !_orchestrationStates.ContainsKey(correlationIdString))
            // {
            //     _logger.LogInformation($"be_wrk_orchestrator: [x] Initiating new Orchestration from FeederResponse (CorrelationId: {correlationIdString})");
            //     var state = new OrchestrationState { FeederResponse = response, CurrentStep = "Requesting Writer Data" };
            //     _orchestrationStates.TryAdd(correlationIdString, state);
            //     // Then proceed to publish WriterCommand based on response.FetchedData
            // }
            // else
            // {
            //     // Existing logic for continuing an ongoing orchestration
            // }
            // --- END NEW ORCHESTRATION INITIATION LOGIC ---


            // Retrieve the ongoing orchestration state
            // NOTE: If FeederResponse now *starts* the orchestration, then TryGetValue will initially fail.
            // You need logic above to add the state if it's a new orchestration.
            if (!_orchestrationStates.TryGetValue(correlationIdString, out var state))
            {
                // For now, assume it's always meant to be part of an ongoing flow.
                // If this FeederResponse is truly the *start*, then this warning might be expected for the first message.
                // Revisit this logic based on how orchestrations are actually initiated.
                 _logger.LogWarning($"be_wrk_orchestrator: [!] Received FeederResponse for unknown CorrelationId: {correlationIdString}. This might be an initial trigger or an out-of-sequence response. Nacking for now.");
                 _rabbitMqService.Nack(context.DeliveryTag, requeue: false);
                 return;
            }

            _logger.LogInformation($"be_wrk_orchestrator: [<-] Received FeederResponse for CorrelationId: {correlationIdString}. IsSuccess: {response.IsSuccess}");

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

            var orchestrationResponse = new OrchestratorResponse // Changed local variable name to avoid conflict with type name
            {
                CorrelationId = response.CorrelationId, // CorrelationId on OrchestrationResponse should match the Guid type
                IsSuccess = response.IsSuccess,
                FinalMessage = response.IsSuccess ? "Orchestration completed successfully." : "Orchestration failed at writer step.",
                PersistedItemId = response.PersistedItemId, // Pass along the ID from writer
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
                _rabbitMqService.Ack(context.DeliveryTag); // Acknowledge writer response message
                _logger.LogInformation($"be_wrk_orchestrator: [->] Orchestration process completed for CorrelationId: {correlationIdString}. Final response not published to a dedicated orchestrator queue as per design.");
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

                // Publish a final failure response
                var orchestrationResponse = new OrchestratorResponse // Changed local variable name to avoid conflict with type name
                {
                    CorrelationId = Guid.Parse(correlationId), // Convert string back to Guid for the message property
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