package main

import (
	context "context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"

	pb "fivetran.com/fivetran_sdk/proto"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/protobuf/proto"
)

const INFO = "INFO"
const WARNING = "WARNING"
const SEVERE = "SEVERE"

var port = flag.Int("port", 50051, "The server port")

type MyState struct {
	Cursor int32 `json:"cursor"`
}

type server struct {
	pb.UnimplementedSourceConnectorServer
}

func (s *server) Update(in *pb.UpdateRequest, stream pb.SourceConnector_UpdateServer) error {
	config := in.Configuration
	selection := in.Selection
	state_json := in.GetStateJson()
	state := MyState{}
	json.Unmarshal([]byte(state_json), &state)

	message := fmt.Sprintf("config: %s, selection: %s, state_json: %s, mystate: %+v", config, selection, state_json, state)
	LogMessage(INFO, message)

	// -- Send a log message
	LogMessage(WARNING, "Sync STARTING")

	// -- Send UPSERT records
	schemaName := "schema1"
	for i := 0; i < 3; i++ {
		stream.Send(&pb.UpdateResponse{
			Operation: &pb.UpdateResponse_Record{
				Record: &pb.Record{
					SchemaName: &schemaName,
					TableName:  "table1",
					Type:       pb.RecordType_UPSERT,
					Data: map[string]*pb.ValueType{
						"a1": {Inner: &pb.ValueType_String_{String_: "a-" + strconv.Itoa(i)}},
						"a2": {Inner: &pb.ValueType_Double{Double: float64(i) * float64(0.234)}},
					},
				},
			},
		})

		state.Cursor++
	}

	LogMessage(INFO, "Completed sending upsert records")

	// -- Send UPDATE record
	stream.Send(&pb.UpdateResponse{
		Operation: &pb.UpdateResponse_Record{
			Record: &pb.Record{
				SchemaName: &schemaName,
				TableName:  "table1",
				Type:       pb.RecordType_UPDATE,
				Data: map[string]*pb.ValueType{
					"a1": {Inner: &pb.ValueType_String_{String_: "a-0"}},
					"a2": {Inner: &pb.ValueType_Double{Double: float64(110.234)}},
				},
			},
		},
	})
	state.Cursor++

	LogMessage(INFO, "Completed sending update records")

	// -- Send DELETE record
	stream.Send(&pb.UpdateResponse{
		Operation: &pb.UpdateResponse_Record{
			Record: &pb.Record{
				SchemaName: &schemaName,
				TableName:  "table1",
				Type:       pb.RecordType_DELETE,
				Data: map[string]*pb.ValueType{
					"a1": {Inner: &pb.ValueType_String_{String_: "a-2"}},
				},
			},
		},
	})
	state.Cursor++

	LogMessage(WARNING, "Sample warning message: Completed sending delete records")

	// Serialize state from struct to JSON string
	new_state_json, _ := json.Marshal(state)
	new_state := string(new_state_json)
	log.Println("new_state: ", new_state)

	// -- Send Checkpoint
	stream.Send(&pb.UpdateResponse{
		Operation: &pb.UpdateResponse_Checkpoint{
			Checkpoint: &pb.Checkpoint{
				StateJson: new_state,
			},
		},
	})

	// -- Send a log message
	LogMessage(WARNING, "Sync DONE")

	LogMessage(SEVERE, "Sample severe message: Update call completed")
	// End the RPC call
	return nil
}

func (s *server) Schema(ctx context.Context, in *pb.SchemaRequest) (*pb.SchemaResponse, error) {
	config := in.Configuration
	log.Println(config)

	return &pb.SchemaResponse{
		Response: &pb.SchemaResponse_WithSchema{
			WithSchema: &pb.SchemaList{
				Schemas: []*pb.Schema{
					{
						Name: "schema1",
						Tables: []*pb.Table{
							{
								Name: "table1",
								Columns: []*pb.Column{
									{
										Name:       "a1",
										Type:       pb.DataType_UNSPECIFIED,
										PrimaryKey: true,
									},
									{
										Name:       "a2",
										Type:       pb.DataType_DOUBLE,
										PrimaryKey: false,
									},
								},
							},
							{
								Name: "table2",
								Columns: []*pb.Column{
									{
										Name:       "b1",
										Type:       pb.DataType_STRING,
										PrimaryKey: true,
									},
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func (s *server) ConfigurationForm(ctx context.Context, in *pb.ConfigurationFormRequest) (*pb.ConfigurationFormResponse, error) {

	return &pb.ConfigurationFormResponse{
		SchemaSelectionSupported: true,
		TableSelectionSupported:  true,
		Fields: []*pb.FormField{
			{
				Name:        "apiBaseURL",
				Label:       "API base URL",
				Description: proto.String("Enter the base URL for the API you're connecting to"),
				Required:    proto.Bool(true),
				Type: &pb.FormField_TextField{
					TextField: pb.TextField_PlainText,
				},
				Placeholder: proto.String("api_base_url"),
			},
			{
				Name:        "authenticationMethod",
				Label:       "Authentication Method",
				Description: proto.String("Choose the preferred authentication method to securely access the API"),
				Required:    proto.Bool(true),
				Type: &pb.FormField_DropdownField{
					DropdownField: &pb.DropdownField{
						DropdownField: []string{"OAuth2.0", "API Key", "Basic Auth", "None"},
					},
				},
				DefaultValue: proto.String("None"),
			},
			{
				Name:  "doesNotMatter",
				Label: "It won't be used",
				Type: &pb.FormField_ConditionalFields{
					ConditionalFields: &pb.ConditionalFields{
						Condition: &pb.VisibilityCondition{
							ConditionField: "authenticationMethod",
							VisibleWhen: &pb.VisibilityCondition_StringValue{
								StringValue: "OAuth2.0",
							},
						},
						Fields: []*pb.FormField{
							{
								Name:        "clientId",
								Label:       "Client Id",
								Type:        &pb.FormField_TextField{TextField: pb.TextField_Password},
								Placeholder: proto.String("your_client_id_here"),
							},
							{
								Name:        "clientSecret",
								Label:       "Client Secret",
								Type:        &pb.FormField_TextField{TextField: pb.TextField_Password},
								Placeholder: proto.String("your_client_secret_here"),
							},
						},
					},
				},
			},
			{
				Name:  "doesNotMatter",
				Label: "It won't be used",
				Type: &pb.FormField_ConditionalFields{
					ConditionalFields: &pb.ConditionalFields{
						Condition: &pb.VisibilityCondition{
							ConditionField: "authenticationMethod",
							VisibleWhen: &pb.VisibilityCondition_StringValue{
								StringValue: "API Key",
							},
						},
						Fields: []*pb.FormField{
							{
								Name:        "apiKey",
								Label:       "API Key",
								Type:        &pb.FormField_TextField{TextField: pb.TextField_Password},
								Placeholder: proto.String("your_api_key_here"),
							},
						},
					},
				},
			},
			{
				Name:  "doesNotMatter",
				Label: "It won't be used",
				Type: &pb.FormField_ConditionalFields{
					ConditionalFields: &pb.ConditionalFields{
						Condition: &pb.VisibilityCondition{
							ConditionField: "authenticationMethod",
							VisibleWhen: &pb.VisibilityCondition_StringValue{
								StringValue: "Basic Auth",
							},
						},
						Fields: []*pb.FormField{
							{
								Name:        "username",
								Label:       "Username",
								Type:        &pb.FormField_TextField{TextField: pb.TextField_PlainText},
								Placeholder: proto.String("your_username_here"),
							},
							{
								Name:        "password",
								Label:       "Password",
								Type:        &pb.FormField_TextField{TextField: pb.TextField_Password},
								Placeholder: proto.String("your_password_here"),
							},
						},
					},
				},
			},
			{
				Name:  "apiVersion",
				Label: "API Version",
				Type: &pb.FormField_DropdownField{
					DropdownField: &pb.DropdownField{
						DropdownField: []string{"v1", "v2", "v3"},
					},
				},
				DefaultValue: proto.String("v2"),
			},
			{
				Name:  "shouldAddMetrics",
				Label: "Enable Metrics?",
				Type:  &pb.FormField_ToggleField{ToggleField: &pb.ToggleField{}},
			},
			{
				Name:        "uploadFile",
				Label:       "Upload Configuration File",
				Description: proto.String("Upload a configuration file (e.g., JSON, YAML, or certificate)"),
				Type: &pb.FormField_UploadField{
					UploadField: &pb.UploadField{
						AllowedFileType:   []string{".json", ".yaml", ".yml", ".pem", ".crt"},
						MaxFileSizeBytes:  1048576, // 1 MB
					},
				},
			},
		},
		Tests: []*pb.ConfigurationTest{
			{
				Name:  "connect",
				Label: "Test connection",
			},
			{
				Name:  "select",
				Label: "Test selection",
			},
		},
	}, nil
}

func (s *server) Test(ctx context.Context, in *pb.TestRequest) (*pb.TestResponse, error) {
	config := in.Configuration
	log.Println(config)

	log.Printf("test name: %v", in.Name)
	return &pb.TestResponse{
		Response: &pb.TestResponse_Success{
			Success: true,
		},
	}, nil
}

func LogMessage(level string, message string) {
	log := map[string]interface{}{
		"level":          level,
		"message":        message,
		"message-origin": "sdk_connector",
	}
	logJSON, _ := json.Marshal(log)
	fmt.Println(string(logJSON))
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterSourceConnectorServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
