package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/ThisaraWeerakoon/Synpase-Go-Connector-PoC/protocol"
)

// ConnectorDefinition holds static configuration for a connector type.
type ConnectorDefinition struct {
	Name                                string                 `json:"name"`
	ExecutablePathRelativeToConnectorsDir string                 `json:"executable_path_relative_to_connectors_dir"`
	DefaultPort                         int                    `json:"default_port"`
	DefaultConfig                       map[string]interface{} `json:"default_config"`
}

// RunningConnectorInstance holds runtime information about an active connector process.
type RunningConnectorInstance struct {
	Definition ConnectorDefinition
	Cmd        *exec.Cmd
	Port       int // Actual port, might differ from default if dynamic allocation is used
	mu         sync.Mutex
	// In a real system, you'd have a client pool or persistent connection here
}

// ConnectorManager manages the lifecycle and interactions with connectors.
type ConnectorManager struct {
	connectorDefinitions map[string]ConnectorDefinition
	runningInstances     map[string]*RunningConnectorInstance
	connectorsBaseDir    string // e.g., "./connectors"
	instanceMutex        sync.RWMutex
}

// NewConnectorManager creates a new manager and loads connector definitions.
func NewConnectorManager(definitionsDir string, connectorsBaseDir string) (*ConnectorManager, error) {
	cm := &ConnectorManager{
		connectorDefinitions: make(map[string]ConnectorDefinition),
		runningInstances:     make(map[string]*RunningConnectorInstance),
		connectorsBaseDir:    connectorsBaseDir,
	}

	files, err := ioutil.ReadDir(definitionsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read connector definitions directory '%s': %w", definitionsDir, err)
	}

	for _, file := range files {
		if filepath.Ext(file.Name()) == ".json" {
			filePath := filepath.Join(definitionsDir, file.Name())
			data, err := ioutil.ReadFile(filePath)
			if err != nil {
				log.Printf("Warn: Could not read connector definition %s: %v", filePath, err)
				continue
			}
			var def ConnectorDefinition
			if err := json.Unmarshal(data, &def); err != nil {
				log.Printf("Warn: Could not parse connector definition %s: %v", filePath, err)
				continue
			}
			cm.connectorDefinitions[def.Name] = def
			log.Printf("ConnectorManager: Loaded definition for connector '%s'", def.Name)
		}
	}
	return cm, nil
}

func (cm *ConnectorManager) getOrStartInstance(connectorName string) (*RunningConnectorInstance, error) {
	cm.instanceMutex.RLock()
	instance, exists := cm.runningInstances[connectorName]
	cm.instanceMutex.RUnlock()

	if exists {
		// Basic check: If process is nil or exited, try to restart
		if instance.Cmd == nil || (instance.Cmd.ProcessState != nil && instance.Cmd.ProcessState.Exited()) {
			log.Printf("ConnectorManager: Instance '%s' found but process exited or nil. Attempting restart.", connectorName)
			// Fall through to start logic by marking exists as false effectively
		} else {
			return instance, nil // Healthy and running
		}
	}
	
	// Acquire write lock to start a new instance or restart an exited one
	cm.instanceMutex.Lock()
	defer cm.instanceMutex.Unlock()

	// Double check after acquiring write lock
	instance, exists = cm.runningInstances[connectorName]
     if exists && instance.Cmd != nil && (instance.Cmd.ProcessState == nil || !instance.Cmd.ProcessState.Exited()) {
         return instance, nil // Another goroutine might have started it
     }

	def, ok := cm.connectorDefinitions[connectorName]
	if !ok {
		return nil, fmt.Errorf("connector definition for '%s' not found", connectorName)
	}

	executablePath := filepath.Join(cm.connectorsBaseDir, def.ExecutablePathRelativeToConnectorsDir)
     // Handle .exe for Windows
     if _, err := os.Stat(executablePath); os.IsNotExist(err) {
         if _, errExe := os.Stat(executablePath + ".exe"); errExe == nil {
             executablePath += ".exe"
         } else {
             return nil, fmt.Errorf("connector executable not found at %s or %s.exe", executablePath, executablePath)
         }
     }


	// For PoC, port is fixed. Real system: dynamic port allocation + registration.
	port := def.DefaultPort
	cmd := exec.Command(executablePath, fmt.Sprintf("-port=%d", port))
	cmd.Stdout = os.Stdout // For PoC, pipe to server's stdio
	cmd.Stderr = os.Stderr

	log.Printf("ConnectorManager: Starting connector '%s' with command: %s", connectorName, cmd.String())
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start connector '%s': %w", connectorName, err)
	}

	newInstance := &RunningConnectorInstance{
		Definition: def,
		Cmd:        cmd,
		Port:       port,
	}
	cm.runningInstances[connectorName] = newInstance

	// Crude readiness check: wait a bit. Real system: health check endpoint on connector.
	time.Sleep(1 * time.Second)
	log.Printf("ConnectorManager: Connector '%s' started (PID: %d) on port %d", connectorName, cmd.Process.Pid, port)
	return newInstance, nil
}

// Invoke sends an operation to a connector.
func (cm *ConnectorManager) Invoke(
	connectorName string,
	operationName string,
	connectorConfigOverride map[string]interface{}, // Optional override for default config
	operationParams map[string]interface{},
	msgCtxIn protocol.MessageContext,
) (protocol.ConnectorOperationResponse, error) {

	instance, err := cm.getOrStartInstance(connectorName)
	if err != nil {
		return protocol.ConnectorOperationResponse{Success: false, ErrorMessage: err.Error()}, err
	}

	instance.mu.Lock() // Serialize operations on a single instance for simplicity
	defer instance.mu.Unlock()

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", instance.Port), 5*time.Second)
	if err != nil {
		// Mark for potential restart if connection fails
		if instance.Cmd != nil && instance.Cmd.ProcessState == nil { // If process not already marked exited
			log.Printf("ConnectorManager: Failed to connect to '%s' (PID: %d). Marking for potential restart.", connectorName, instance.Cmd.Process.Pid)
			// A more robust system would kill and restart or have a backoff
		}
		return protocol.ConnectorOperationResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to connect to connector '%s': %v", connectorName, err)}, err
	}
	defer conn.Close()

	currentConnectorConfig := instance.Definition.DefaultConfig
	if connectorConfigOverride != nil { // Merge/override
		if currentConnectorConfig == nil {
			currentConnectorConfig = make(map[string]interface{})
		}
		for k, v := range connectorConfigOverride {
			currentConnectorConfig[k] = v
		}
	}

	req := protocol.ConnectorOperationRequest{
		ConnectorName:   connectorName,
		OperationName:   operationName,
		ConnectorConfig: currentConnectorConfig,
		OperationParams: operationParams,
		MessageContextIn: msgCtxIn,
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	log.Printf("ConnectorManager: Sending request to %s for op %s", connectorName, operationName)
	if err := encoder.Encode(req); err != nil {
		return protocol.ConnectorOperationResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to send request to connector '%s': %v", connectorName, err)}, err
	}

	var resp protocol.ConnectorOperationResponse
	if err := decoder.Decode(&resp); err != nil {
		return protocol.ConnectorOperationResponse{Success: false, ErrorMessage: fmt.Sprintf("failed to decode response from connector '%s': %v", connectorName, err)}, err
	}
	log.Printf("ConnectorManager: Received response from %s for op %s. Success: %t", connectorName, operationName, resp.Success)
	return resp, nil
}

// ShutdownAll terminates all running connector instances.
func (cm *ConnectorManager) ShutdownAll() {
	cm.instanceMutex.Lock()
	defer cm.instanceMutex.Unlock()
	log.Println("ConnectorManager: Shutting down all connector instances...")
	for name, instance := range cm.runningInstances {
		if instance.Cmd != nil && instance.Cmd.Process != nil {
			log.Printf("ConnectorManager: Terminating connector '%s' (PID: %d)", name, instance.Cmd.Process.Pid)
			if err := instance.Cmd.Process.Signal(os.Interrupt); err != nil { // SIGINT
				log.Printf("ConnectorManager: Failed to send SIGINT to %s, attempting SIGKILL: %v", name, err)
				_ = instance.Cmd.Process.Kill()
			}
			instance.Cmd.Wait() // Wait for the process to exit
			log.Printf("ConnectorManager: Connector '%s' shut down.", name)
		}
	}
	cm.runningInstances = make(map[string]*RunningConnectorInstance)
}