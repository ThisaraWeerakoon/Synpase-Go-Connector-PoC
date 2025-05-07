package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
	"github.com/google/uuid"


	"github.com/ThisaraWeerakoon/Synpase-Go-Connector-PoC/protocol" 
)

func main() {
	log.Println("Synapse Go Server: Initializing...")

	// Define paths relative to the server executable's location or use absolute paths.
	// For this PoC structure, if running 'synapse-server' from 'synapse-server/' dir:
	definitionsDir := "../connector-definitions"
	connectorsBaseDir := "../connectors"


	cm, err := NewConnectorManager(definitionsDir, connectorsBaseDir)
	if err != nil {
		log.Fatalf("Synapse Go Server: Failed to initialize ConnectorManager: %v", err)
	}
	log.Println("Synapse Go Server: ConnectorManager initialized.")

	// --- Simulate a mediation flow ---
	go func() {
		time.Sleep(2 * time.Second) // Wait for connector to potentially start

		log.Println("--- Synapse Go Server: Simulating 'createFile' operation ---")
		msgCtxCreate := protocol.MessageContext{
			MessageID:  uuid.NewString(),
			Payload:    []byte("<original_payload>data</original_payload>"),
			Properties: map[string]interface{}{"flowName": "fileCreateFlow"},
			Headers:    map[string]string{"X-Request-ID": "123"},
		}
		createParams := map[string]interface{}{
			"filename": "poc_output.txt",
			"content":  "Hello from Synapse Go Connector Framework! Timestamp: " + time.Now().String(),
		}
		// Optional connector config override for this specific call (can be nil)
		// var connectorConfigOverride map[string]interface{} = map[string]interface{}{"baseDirectory": "./custom_connector_data"}

		respCreate, err := cm.Invoke("SimpleFileConnector", "create", nil, createParams, msgCtxCreate)
		if err != nil {
			log.Printf("Synapse Go Server: Error invoking 'createFile': %v", err)
		} else {
			if respCreate.Success {
				log.Printf("Synapse Go Server: 'createFile' succeeded. Properties: %v", respCreate.MessageContextOut.Properties)
			} else {
				log.Printf("Synapse Go Server: 'createFile' failed: %s", respCreate.ErrorMessage)
			}
		}

		log.Println("--- Synapse Go Server: Simulating 'readFile' operation ---")
		msgCtxRead := protocol.MessageContext{
			MessageID:  uuid.NewString(),
			Properties: map[string]interface{}{"flowName": "fileReadFlow"},
		}
		readParams := map[string]interface{}{
			"filename": "poc_output.txt",
		}
		respRead, err := cm.Invoke("SimpleFileConnector", "read", nil, readParams, msgCtxRead)
		if err != nil {
			log.Printf("Synapse Go Server: Error invoking 'readFile': %v", err)
		} else {
			if respRead.Success {
				log.Printf("Synapse Go Server: 'readFile' succeeded. Payload: %s", string(respRead.MessageContextOut.Payload))
				log.Printf("Synapse Go Server: 'readFile' properties: %v", respRead.MessageContextOut.Properties)
			} else {
				log.Printf("Synapse Go Server: 'readFile' failed: %s", respRead.ErrorMessage)
			}
		}
	}()

	// --- Graceful shutdown ---
	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel, syscall.SIGINT, syscall.SIGTERM)
	<-quitChannel

	log.Println("Synapse Go Server: Received shutdown signal. Cleaning up...")
	cm.ShutdownAll()
	log.Println("Synapse Go Server: Shutdown complete.")
}