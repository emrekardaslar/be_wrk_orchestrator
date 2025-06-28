# be_wrk_orchestrator

This project is the orchestrator service for the OrchestrationDemo solution. It coordinates a distributed workflow between the Feeder and Writer services using RabbitMQ for messaging.

## Features

- Declares and purges all required RabbitMQ queues on startup
- Starts a new orchestration process by sending a command to the Feeder service
- Listens for responses from Feeder and Writer services
- Tracks the state of each orchestration using a correlation ID
- Handles errors and logs all major events
- Automatically starts a new orchestration after each Writer completes

## Project Structure

- `OrchestratorWorker.cs` - Main background service implementing the orchestration logic
- `Program.cs` - Application entry point and host configuration
- `appsettings.json` - Configuration file for RabbitMQ and other settings

## Running the Service

1. Ensure RabbitMQ is running and accessible as configured in `appsettings.json`.
2. Build and run the project:
   ```sh
   dotnet run
   ```
3. The orchestrator will declare and purge queues, then begin orchestrating workflows between Feeder and Writer.

## Configuration

Edit `appsettings.json` or `appsettings.Development.json` to set RabbitMQ connection details and other settings.

## Development

- .NET 8.0
- Uses dependency injection and background services
- Requires the `core_lib_messaging` library for RabbitMQ integration

## Related Projects

- `be_wrk_feeder` - Feeder service
- `be_wrk_writer` - Writer service
- `core_lib_messaging` - Shared messaging library

---

**Note:**  
This orchestrator is designed for demo and development purposes. In production, consider adding persistence, health checks, and more robust error handling.