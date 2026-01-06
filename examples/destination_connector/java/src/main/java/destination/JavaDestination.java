package destination;

import io.grpc.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;

/**
 * Example Plugin Connector (gRPC server)
 * In production, it will be stored as a container image
 */
public class JavaDestination {

    /**
     * Check if a port is already in use.
     */
    private static boolean isPortInUse(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }

    public static void main(String[] args) {
        int port = 50052;
        for(int i=0;i<args.length;i++) if (args[i].equals("--port")) port = Integer.parseInt(args[i + 1]);

        // Check if port is already in use BEFORE initializing database connection
        if (isPortInUse(port)) {
            throw new RuntimeException("Port " + port + " is already in use. Another server may be running.");
        }

        DestinationServiceImpl service = new DestinationServiceImpl();
        Server server = ServerBuilder
                .forPort(port)
                .addService(service)
                .build();

        // Add shutdown hook to handle Ctrl+C (SIGINT)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nReceived shutdown signal...");
            System.out.println("Shutting down server...");

            // Stop server first with grace period to allow in-flight requests to complete
            if (server != null && !server.isShutdown()) {
                server.shutdown();
                try {
                    if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                        server.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    server.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }

            // Close database connection after all requests have finished
            if (service.getDbHelper() != null) {
                service.getDbHelper().close();
            }

            System.out.println("Destination gRPC server terminated...");
        }));

        try {
            server.start();
            System.out.println("Destination gRPC server started on port " + port + "...");
            server.awaitTermination();
        } catch (InterruptedException e) {
            System.out.println("\nReceived shutdown signal during wait...");
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            System.err.println("Failed to start server: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
