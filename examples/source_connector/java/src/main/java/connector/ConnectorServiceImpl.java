package connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fivetran_sdk.v2.*;
import fivetran_sdk.v2.Record;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;

public class ConnectorServiceImpl extends SourceConnectorGrpc.SourceConnectorImplBase {

    private static final Logger logger = getLogger();

    // Get the configured logger
    private static Logger getLogger() {
        Logger logger = Logger.getLogger(ConnectorServiceImpl.class.getName());
        configureLogging();
        return logger;
    }


    // Remove existing console handlers
    private static void removeExistingConsoleHandlers() {
        Logger rootLogger = Logger.getLogger("");
        for (Handler handler : rootLogger.getHandlers()) {
            if (handler instanceof ConsoleHandler) {
                rootLogger.removeHandler(handler);
            }
        }
    }

    // Create a new console handler and configure it to STDOUT
    private static ConsoleHandler createConsoleHandler() {
        ConsoleHandler stdoutHandler = new ConsoleHandler();
        stdoutHandler.setLevel(Level.ALL);
        stdoutHandler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                String level = record.getLevel().getName();
                String message = record.getMessage();
                String jsonMessage = message;
                ObjectMapper objectMapper = new ObjectMapper();
                try {
                    jsonMessage = objectMapper.writeValueAsString(message);
                } catch (JsonProcessingException e) {
                    System.out.println("Error while converting message to JSON");
                }
                return String.format("{\"level\":\"%s\", \"message\": %s, \"message-origin\": \"sdk_connector\"}%n",
                        level, jsonMessage);
            }
        });

        // Filter log messages to only include INFO, WARNING, and SEVERE
        stdoutHandler.setFilter(record -> {
            Level level = record.getLevel();
            return level == Level.INFO || level == Level.WARNING || level == Level.SEVERE;
        });

        return stdoutHandler;
    }

    private static void configureLogging() {
        removeExistingConsoleHandlers();
        ConsoleHandler stdoutHandler = createConsoleHandler();
        Logger rootLogger = Logger.getLogger("");
        rootLogger.addHandler(stdoutHandler);
        rootLogger.setLevel(Level.ALL);
    }

    @Override
    public void configurationForm(ConfigurationFormRequest request, StreamObserver<ConfigurationFormResponse> responseObserver) {
        logger.info("Started fetching configuration form");
        ConfigurationFormResponse formResponse = getConfigurationForm();
        responseObserver.onNext(formResponse);

        logger.info("Fetching configuration form completed");
        responseObserver.onCompleted();
    }

    private ConfigurationFormResponse getConfigurationForm(){
        FormField apiBaseURL = FormField.newBuilder()
                .setName("apiBaseURL")
                .setLabel("API base URL")
                .setDescription("Enter the base URL for the API you're connecting to")
                .setRequired(true)
                .setTextField(TextField.PlainText)
                .setPlaceholder("api_base_url")
                .build();

        FormField authenticationMethods = FormField.newBuilder()
                .setName("authenticationMethod")
                .setLabel("Authentication Method")
                .setDescription("Choose the preferred authentication method to securely access the API")
                .setDropdownField(
                        DropdownField.newBuilder()
                                .addAllDropdownField(Arrays.asList("OAuth2.0", "API Key", "Basic Auth", "None"))
                                .build())
                .setDefaultValue("None")
                .build();

        FormField apiKey = FormField.newBuilder()
                .setName("apiKey")
                .setLabel("Api Key")
                .setTextField(TextField.Password)
                .setPlaceholder("your_api_key_here")
                .build();

        FormField clientId = FormField.newBuilder()
                .setName("clientId")
                .setLabel("Client Id")
                .setTextField(TextField.Password)
                .setPlaceholder("your_client_id_here")
                .build();

        FormField clientSecret = FormField.newBuilder()
                .setName("clientSecret")
                .setLabel("Client Secret")
                .setTextField(TextField.Password)
                .setPlaceholder("your_client_secret_here")
                .build();

        FormField userName = FormField.newBuilder()
                .setName("username")
                .setLabel("Username")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_username_here")
                .build();

        FormField password = FormField.newBuilder()
                .setName("password")
                .setLabel("Password")
                .setTextField(TextField.Password)
                .setPlaceholder("your_password_here")
                .build();

        FormField apiVersions = FormField.newBuilder()
                .setName("apiVersion")
                .setLabel("Api Version")
                .setDropdownField(
                        DropdownField.newBuilder().addAllDropdownField(Arrays.asList("v1","v2","v3")).build())
                .setDefaultValue("v2")
                .build();

        FormField addMetrics = FormField.newBuilder()
                .setName("shouldAddMetrics")
                .setLabel("Enable Metrics?")
                .setToggleField(ToggleField.newBuilder().build())
                .build();

        FormField uploadFile = FormField.newBuilder()
                .setName("uploadFile")
                .setLabel("Upload Configuration File")
                .setDescription("Upload a configuration file (e.g., JSON, YAML, or certificate)")
                .setUploadField(UploadField.newBuilder()
                        .addAllAllowedFileType(Arrays.asList(".json", ".yaml", ".yml", ".pem", ".crt"))
                        .setMaxFileSizeBytes(1048576) // 1 MB
                        .build())
                .build();


        // Conditional Field for OAuth
        VisibilityCondition visibilityCondition1 = VisibilityCondition.newBuilder()
                .setConditionField("authenticationMethod")
                .setStringValue("OAuth2.0")
                .build();

        FormField conditionalField1 = FormField.newBuilder()
                .setName("doesNotMatter")
                .setLabel("It won't be used")
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(visibilityCondition1)
                                .addAllFields(Arrays.asList(clientId, clientSecret))
                                .build())
                .build();

        // Conditional Field for API Key authentication method
        VisibilityCondition visibilityCondition2 = VisibilityCondition.newBuilder()
                .setConditionField("authenticationMethod")
                .setStringValue("API Key")
                .build();

        FormField conditionalField2 = FormField.newBuilder()
                .setName("doesNotMatter")
                .setLabel("It won't be used")
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(visibilityCondition2)
                                .addAllFields(Arrays.asList(apiKey))
                                .build())
                .build();

        // Conditional Field for Basic Auth
        VisibilityCondition visibilityCondition3 = VisibilityCondition.newBuilder()
                .setConditionField("authenticationMethod")
                .setStringValue("Basic Auth")
                .build();

        FormField conditionalField3 = FormField.newBuilder()
                .setName("doesNotMatter")
                .setLabel("It won't be used")
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(visibilityCondition3)
                                .addAllFields(Arrays.asList(userName, password))
                                .build())
                .build();

        return ConfigurationFormResponse.newBuilder()
                .setSchemaSelectionSupported(true)
                .setTableSelectionSupported(true)
                .addAllFields(
                        Arrays.asList(
                                apiBaseURL,
                                authenticationMethods,
                                conditionalField1,
                                conditionalField2,
                                conditionalField3,
                                apiVersions,
                                addMetrics,
                                uploadFile))
                .addAllTests(
                        Arrays.asList(
                                ConfigurationTest.newBuilder().setName("connect").setLabel("Tests connection").build(),
                                ConfigurationTest.newBuilder().setName("select").setLabel("Tests selection").build()))
                .build();
    }

    @Override
    public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {

        Map<String, String> configuration = request.getConfigurationMap();

        // Name of the test to be run
        String testName = request.getName();
        String message = String.format("test name: %s", testName);
        logger.info(message);

        responseObserver.onNext(TestResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void schema(SchemaRequest request, StreamObserver<SchemaResponse> responseObserver) {

        logger.warning("Sample warning message while fetching schema");
        Map<String, String> configuration = request.getConfigurationMap();

        TableList tableList = TableList.newBuilder()
                .addAllTables(Arrays.asList(
                        Table.newBuilder().setName("table1").addAllColumns(
                                Arrays.asList(
                                        Column.newBuilder().setName("a1").setType(DataType.UNSPECIFIED).setPrimaryKey(true).build(),
                                        Column.newBuilder().setName("a2").setType(DataType.DOUBLE).build())
                        ).build(),
                        Table.newBuilder().setName("table2").addAllColumns(
                                Arrays.asList(
                                        Column.newBuilder().setName("b1").setType(DataType.STRING).setPrimaryKey(true).build(),
                                        Column.newBuilder().setName("b2").setType(DataType.UNSPECIFIED).build())
                        ).build())
                ).build();

        responseObserver.onNext(SchemaResponse.newBuilder().setWithoutSchema(tableList).build());
        responseObserver.onCompleted();
    }

    @Override
    public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();
        String state_json = request.hasStateJson() ? request.getStateJson() : "{}";
        Selection selection = request.hasSelection() ? request.getSelection() : null;

        ObjectMapper mapper = new ObjectMapper();
        UpdateResponse.Builder responseBuilder = UpdateResponse.newBuilder();

        try {
            State state = mapper.readValue(state_json, State.class);

            // -- Send a log message
            logger.warning("Sync STARTING");

            // -- Send UPSERT records
            Record.Builder recordBuilder = Record.newBuilder();
            Map<String, ValueType> row = new HashMap<>();
            for (int i=0; i<3; i++) {
                responseBuilder.clear();
                recordBuilder.clear();

                row.clear();
                row.put("a1", ValueType.newBuilder().setString("a-" + i).build());
                row.put("a2", ValueType.newBuilder().setDouble(i * 0.234d).build());

                responseObserver.onNext(responseBuilder
                        .setRecord(recordBuilder
                                        .setTableName("table1")
                                        .setType(RecordType.UPSERT)
                                        .putAllData(row)
                                        .build())
                                .build());

                state.cursor += 1;
            }

            // -- Send UPDATE record
            responseBuilder.clear();
            recordBuilder.clear();
            row.clear();
            row.put("a1", ValueType.newBuilder().setString("a-0").build());
            row.put("a2", ValueType.newBuilder().setDouble(110.234d).build());
            responseObserver.onNext(responseBuilder
                    .setRecord(recordBuilder
                                    .setTableName("table1")
                                    .setType(RecordType.UPDATE)
                                    .putAllData(row)
                                    .build())
                            .build());
            state.cursor += 1;

            // -- Send DELETE record
            responseBuilder.clear();
            recordBuilder.clear();
            row.clear();
            row.put("a1", ValueType.newBuilder().setString("a-2").build());
            responseObserver.onNext(responseBuilder
                    .setRecord(recordBuilder
                                    .setTableName("table1")
                                    .setType(RecordType.DELETE)
                                    .putAllData(row)
                                    .build())
                            .build());
            state.cursor += 1;

            // -- Send checkpoint
            String newState = mapper.writeValueAsString(state);
            Checkpoint checkpoint = Checkpoint.newBuilder().setStateJson(newState).build();
            responseObserver.onNext(responseBuilder.setCheckpoint(checkpoint).build());

            // -- Send a log message
            logger.warning("Sync DONE");
        } catch (JsonProcessingException e) {
            String message = e.getMessage();
            logger.severe(message);
            responseObserver.onError(e);
        }

        // End the streaming RPC call
        responseObserver.onCompleted();
    }
}
