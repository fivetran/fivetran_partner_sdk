const grpc = require("@grpc/grpc-js");
const PROTO_PATH_CONNECTOR = "./src/protos/connector_sdk.proto";
var protoLoader = require("@grpc/proto-loader");

const options = {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
};
var packageDefinitionConnector = protoLoader.loadSync(PROTO_PATH_CONNECTOR, options);

const INFO = "INFO";
const WARNING = "WARNING";
const SEVERE = "SEVERE";

const protoDescriptor = grpc.loadPackageDefinition(packageDefinitionConnector);

const connectorSdkProtoV2 = protoDescriptor.fivetran_sdk.v2;

const server = new grpc.Server();

const configurationForm = (call, callback) => {
    logMessage(INFO, "Fetching configuration form")
    callback(null, {
      schema_selection_supported: true,
      table_selection_supported: true,
      fields: [
        {
          name: "apiBaseURL",
          label: "API base URL",
          description: "Enter the base URL for the API you're connecting to",
          required: true,
          text_field: "PlainText",
          placeholder: "api_base_url"
        },
        {
          name: "authenticationMethod",
          label: "Authentication Method",
          description: "Choose the preferred authentication method to securely access the API",
          dropdown_field: {
            dropdown_field: ["OAuth2.0", "API Key", "Basic Auth", "None"]
          },
          default_value: "None"
        },
        {
          name: "doesNotMatter",
          label: "It won't be used",
          conditional_fields: {
            condition: {
              condition_field: "authenticationMethod",
              string_value: "OAuth2.0"
            },
            fields: [
              {
              name: "clientId",
              label: "Client Id",
              text_field: "Password",
              placeholder: "your_client_id_here"
              },
              {
                name: "clientSecret",
                label: "Client Secret",
                text_field: "Password",
                placeholder: "your_client_secret_here"
              }]
          }
        },
        {
          name: "doesNotMatter",
          label: "It won't be used",
          conditional_fields: {
            condition: {
              condition_field: "authenticationMethod",
              string_value: "API Key"
            },
            fields: [
              {
                name: "apiKey",
                label: "API Key",
                text_field: "Password",
                placeholder: "your_api_key_here"
              }
            ]
          }
        },
        {
          name: "doesNotMatter",
          label: "It won't be used",
          conditional_fields: {
            condition: {
              condition_field: "authenticationMethod",
              string_value: "Basic Auth"
            },
            fields: [
              {
              name: "username",
              label: "Username",
              text_field: "PlainText",
              placeholder: "your_username_here"
              },
              {
                name: "password",
                label: "Password",
                text_field: "Password",
                placeholder: "your_password_here"
              },
          ]
          }
        },
        {
          name: "apiVersion",
          label: "API Version",
          dropdown_field: {
            dropdown_field: ["v1", "v2", "v3"]
          },
          default_value: "v2"
        },
        {
          name: "shouldAddMetrics",
          label: "Enable Metrics?",
          toggle_field: {}
        }
      ],
      tests: [
        { name: "connect", label: "Tests connection" },
        { name: "select", label: "Tests selection" }
      ]
    });
  };

  // Implement the Test RPC method
  const test = (call, callback) => {
    const configuration = call.request.configuration;
    const testName = call.request.name;
    logMessage(INFO, `Test name: ${testName}`)
    callback(null, { success: true });
  };

  // Implement the Schema RPC method
  const schema = (call, callback) => {
    logMessage(INFO, "Fetching the schema from the implemented method")
    const tableList = {
      tables: [
        {
          name: "table1",
          columns: [
            { name: "a1", type: "UNSPECIFIED", primary_key: true },
            { name: "a2", type: "DOUBLE" }
          ]
        },
        {
          name: "table2",
          columns: [
            { name: "b1", type: "STRING", primary_key: true },
            { name: "b2", type: "UNSPECIFIED" }
          ]
        }
      ]
    };
    callback(null, { without_schema: tableList });
  };


  // Implement the update RPC method
  const update = (call, callback) => {
    const { configuration, state_json = '{}', selection } = call.request;
    
    const state = JSON.parse(state_json);
  
    const sendResponse = (response) => {
      call.write(response);
    };
  
    try {
      // Send a log message
      logMessage(WARNING, "Sample Warning message: Sync STARTING");

      // Send UPSERT records
      for (let i = 0; i < 3; i++) {
        sendResponse({
          record: {
            table_name: "table1",
            type: "UPSERT",
            data: {
              "a1": {"string": `a-${i}`},
              "a2": {"double": i*0.234}
            }
          }
        });
        state.cursor = (state.cursor || 0) + 1;
      }
  
      // Send UPDATE record
      sendResponse({
        record: {
          table_name: "table1",
          type: "UPDATE",
          data: {
            "a1": { "string": "a-0" },
            "a2": { "double": 110.234} 
          }
        }
      });
      state.cursor = (state.cursor || 0) + 1;
  
      // Send DELETE record
      sendResponse({
        record: {
          table_name: "table1",
          type: "DELETE",
          data: {
            "a1": { "string" : "a-2" }
          }
        }
      });
      state.cursor = (state.cursor || 0) + 1;
  
      // Send checkpoint
      const newState = JSON.stringify(state);
      sendResponse({
        checkpoint: {
          state_json: newState
        }
      });
  
      // Send a log message
      logMessage(SEVERE, "Sample severe message: Sync done")
  
    } catch (error) {
      callback(error);
    }
  
    // End the streaming RPC call
    call.end();
  };

  function logMessage(level, message) {
      console.log(`{"level":"${level}", "message": "${message}", "message-origin": "sdk_connector"}`);
  }


  server.addService(connectorSdkProtoV2.SourceConnector.service, {configurationForm, test, schema, update})

  var args = process.argv.slice(2);
  var port = args.find(arg => arg === "--port") ? parseInt(args[args.indexOf("--port") + 1]) : 50051;
  server.bindAsync(
    '0.0.0.0'.concat(':').concat(port),
    grpc.ServerCredentials.createInsecure(),
    (error, port) => {
      if (error) {
        console.log("Server failed with error: " + error);
      } else {
        console.log(`Server running at http://127.0.0.1:${port}`);
      }
    }
  );