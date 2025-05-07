package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/ThisaraWeerakoon/Synpase-Go-Connector-PoC/protocol"
)

var port *int

func main() {
	port = flag.Int("port", 0, "Port to listen on")
	flag.Parse()

	if *port == 0 {
		log.Fatal("Connector Error: -port flag is required.")
	}

	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		log.Fatalf("Connector Error: Failed to listen on port %d: %v", *port, err)
	}
	defer listener.Close()
	log.Printf("SimpleFileConnector: Listening on port %d", *port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Connector Error: Failed to accept connection: %v", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Println("SimpleFileConnector: New connection accepted")

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	var req protocol.ConnectorOperationRequest
	if err := decoder.Decode(&req); err != nil {
		log.Printf("SimpleFileConnector: Error decoding request: %v", err)
		_ = encoder.Encode(protocol.ConnectorOperationResponse{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Failed to decode request: %v", err),
		})
		return
	}

	log.Printf("SimpleFileConnector: Received operation '%s' for connector '%s'", req.OperationName, req.ConnectorName)

	var resp protocol.ConnectorOperationResponse
	switch req.OperationName {
	case "create":
		resp = createFile(req)
	case "read":
		resp = readFile(req)
	default:
		resp = protocol.ConnectorOperationResponse{
			Success:           false,
			MessageContextOut: req.MessageContextIn,
			ErrorMessage:      fmt.Sprintf("Unknown operation: %s", req.OperationName),
		}
	}

	if err := encoder.Encode(resp); err != nil {
		log.Printf("SimpleFileConnector: Error encoding response: %v", err)
	}
}

func createFile(req protocol.ConnectorOperationRequest) protocol.ConnectorOperationResponse {
	filename, okFile := req.OperationParams["filename"].(string)
	content, okContent := req.OperationParams["content"].(string)

	if !okFile || !okContent {
		return protocol.ConnectorOperationResponse{Success: false, MessageContextOut: req.MessageContextIn, ErrorMessage: "filename or content parameter missing/invalid"}
	}

	baseDir, _ := req.ConnectorConfig["baseDirectory"].(string)
	if baseDir == "" {
		baseDir = "." // Default to current directory if not configured
	}
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return protocol.ConnectorOperationResponse{Success: false, MessageContextOut: req.MessageContextIn, ErrorMessage: fmt.Sprintf("failed to create base directory %s: %v", baseDir, err)}
	}
	fullPath := filepath.Join(baseDir, filename)

	err := ioutil.WriteFile(fullPath, []byte(content), 0644)
	if err != nil {
		return protocol.ConnectorOperationResponse{Success: false, MessageContextOut: req.MessageContextIn, ErrorMessage: fmt.Sprintf("failed to write file %s: %v", fullPath, err)}
	}

	outCtx := req.MessageContextIn
	if outCtx.Properties == nil {
		outCtx.Properties = make(map[string]interface{})
	}
	outCtx.Properties["file.write.path"] = fullPath
	outCtx.Properties["file.write.status"] = "success"

	return protocol.ConnectorOperationResponse{Success: true, MessageContextOut: outCtx}
}

func readFile(req protocol.ConnectorOperationRequest) protocol.ConnectorOperationResponse {
	filename, okFile := req.OperationParams["filename"].(string)
	if !okFile {
		return protocol.ConnectorOperationResponse{Success: false, MessageContextOut: req.MessageContextIn, ErrorMessage: "filename parameter missing/invalid"}
	}
	baseDir, _ := req.ConnectorConfig["baseDirectory"].(string)
	if baseDir == "" {
		baseDir = "."
	}
	fullPath := filepath.Join(baseDir, filename)

	data, err := ioutil.ReadFile(fullPath)
	if err != nil {
		return protocol.ConnectorOperationResponse{Success: false, MessageContextOut: req.MessageContextIn, ErrorMessage: fmt.Sprintf("failed to read file %s: %v", fullPath, err)}
	}
	outCtx := req.MessageContextIn
	outCtx.Payload = data
	if outCtx.Properties == nil {
		outCtx.Properties = make(map[string]interface{})
	}
	outCtx.Properties["file.read.path"] = fullPath
	return protocol.ConnectorOperationResponse{Success: true, MessageContextOut: outCtx}
}