package protocol

// MessageContext represents a simplified message context
type MessageContext struct {
	MessageID  string                 `json:"message_id"`
	Payload    []byte                 `json:"payload"`    // Raw payload (XML, JSON, text, etc.)
	Properties map[string]interface{} `json:"properties"` // Mediation properties
	Headers    map[string]string      `json:"headers"`    // Transport headers
}

// ConnectorOperationRequest is sent from Synapse to the Connector
type ConnectorOperationRequest struct {
	ConnectorName   string                 `json:"connector_name"`
	OperationName   string                 `json:"operation_name"`
	ConnectorConfig map[string]interface{} `json:"connector_config"` // Instance-specific config (e.g., API keys, base URLs)
	OperationParams map[string]interface{} `json:"operation_params"` // Parameters for the specific operation
	MessageContextIn MessageContext        `json:"message_context_in"`
}

// ConnectorOperationResponse is sent from the Connector back to Synapse
type ConnectorOperationResponse struct {
	Success           bool           `json:"success"`
	MessageContextOut MessageContext `json:"message_context_out"` // Potentially modified context
	ErrorMessage      string         `json:"error_message,omitempty"`
}