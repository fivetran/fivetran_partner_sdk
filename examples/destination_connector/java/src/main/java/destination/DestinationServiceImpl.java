package destination;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fivetran_sdk.v2.*;
import io.grpc.stub.StreamObserver;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.*;

public class DestinationServiceImpl extends DestinationConnectorGrpc.DestinationConnectorImplBase {

    private static final Logger logger = getLogger();
    private static final Map<String, Table> tableMap = new HashMap<>();

    // Get the configured logger
    private static Logger getLogger() {
        Logger logger = Logger.getLogger(DestinationServiceImpl.class.getName());
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
                return String.format("{\"level\":\"%s\", \"message\": \"%s\", \"message-origin\": \"sdk_destination\"}%n",
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
        logger.info("Fetching configuration form");
        responseObserver.onNext(getConfigurationForm());

        responseObserver.onCompleted();
    }

    private ConfigurationFormResponse getConfigurationForm() {

        FormField writerType = FormField.newBuilder()
                .setName("writerType")
                .setLabel("Writer Type")
                .setDescription("Choose the destination type")
                .setDropdownField(
                        DropdownField.newBuilder()
                                .addAllDropdownField(Arrays.asList("Database", "File", "Cloud"))
                                .build())
                .setDefaultValue("Database")
                .build();

        FormField host = FormField.newBuilder()
                .setName("host")
                .setLabel("Host")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_host_details")
                .build();

        FormField port = FormField.newBuilder()
                .setName("port")
                .setLabel("Port")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_port_details")
                .build();

        FormField user = FormField.newBuilder()
                .setName("user")
                .setLabel("User")
                .setTextField(TextField.PlainText)
                .setPlaceholder("user_name")
                .build();

        FormField password = FormField.newBuilder()
                .setName("password")
                .setLabel("password")
                .setTextField(TextField.Password)
                .setPlaceholder("your_password")
                .build();

        FormField database = FormField.newBuilder()
                .setName("database")
                .setLabel("Database")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_database_name")
                .build();

        FormField table = FormField.newBuilder()
                .setName("table")
                .setLabel("Table")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_table_name")
                .build();

        FormField filePath = FormField.newBuilder()
                .setName("filePath")
                .setLabel("File Path")
                .setTextField(TextField.PlainText)
                .setPlaceholder("your_file_path")
                .build();

        FormField region = FormField.newBuilder()
                .setName("region")
                .setLabel("Cloud Region")
                .setDescription("Choose the cloud region")
                .setDropdownField(
                        DropdownField.newBuilder()
                                .addAllDropdownField(Arrays.asList("Azure", "AWS", "Google Cloud"))
                                .build())
                .setDefaultValue("Azure")
                .build();

        FormField enableEncryption = FormField.newBuilder()
                .setName("enableEncryption")
                .setDescription("To enable/disable encryption for data transfer")
                .setLabel("Enable Encryption?")
                .setToggleField(ToggleField.newBuilder().build())
                .build();

        // List of Visibility Conditions
        VisibilityCondition visibilityConditionForCloud = VisibilityCondition.newBuilder()
                .setConditionField("writerType")
                .setStringValue("Cloud")
                .build();

        VisibilityCondition visibilityConditionForDatabase = VisibilityCondition.newBuilder()
                .setConditionField("writerType")
                .setStringValue("Database")
                .build();

        VisibilityCondition visibilityConditionForFile = VisibilityCondition.newBuilder()
                .setConditionField("writerType")
                .setStringValue("File")
                .build();

        // List of conditional fields
        // Note: The 'name' and 'label' parameters in the FormField for conditional fields are not used.
        FormField conditionalFieldForCloud = FormField.newBuilder()
                .setName("conditionalFieldForCloud")
                .setLabel("Conditional Field for Cloud")
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(visibilityConditionForCloud)
                                .addAllFields(Arrays.asList(host, port, user, password, region))
                                .build())
                .build();

        FormField conditionalFieldForFile = FormField.newBuilder()
                .setName("conditionalFieldForFile")
                .setLabel("Conditional Field for File")
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(visibilityConditionForFile)
                                .addAllFields(Arrays.asList(host, port, user, password, table, filePath))
                                .build())
                .build();

        FormField conditionalFieldForDatabase = FormField.newBuilder()
                .setName("conditionalFieldForDatabase")
                .setLabel("Conditional Field for Database")
                .setConditionalFields(
                        ConditionalFields.newBuilder()
                                .setCondition(visibilityConditionForDatabase)
                                .addAllFields(Arrays.asList(host, port, user, password, database, table))
                                .build())
                .build();

        return ConfigurationFormResponse.newBuilder()
                .setSchemaSelectionSupported(true)
                .setTableSelectionSupported(true)
                .addAllFields(
                        Arrays.asList(
                                writerType,
                                conditionalFieldForFile,
                                conditionalFieldForCloud,
                                conditionalFieldForDatabase,
                                enableEncryption))
                .addAllTests(
                        Arrays.asList(
                                ConfigurationTest.newBuilder().setName("connect").setLabel("Tests connection").build(),
                                ConfigurationTest.newBuilder().setName("select").setLabel("Tests selection").build()))
                .build();
    }

    @Override
    public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();
        String testName = request.getName();
        String message = String.format("Test Name: %s", testName);
        logger.info(message);

        responseObserver.onNext(TestResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void describeTable(DescribeTableRequest request, StreamObserver<DescribeTableResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();
        DescribeTableResponse response;
        if (!tableMap.containsKey(request.getTableName())) {
            response = DescribeTableResponse.newBuilder().setNotFound(true).build();
        } else {
            response = DescribeTableResponse.newBuilder().setTable(tableMap.get(request.getTableName())).build();
        }

        responseObserver.onNext(response);
        logger.severe("Sample Severe log: Completed describe Table method");
        responseObserver.onCompleted();
    }

    @Override
    public void createTable(CreateTableRequest request, StreamObserver<CreateTableResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();

        String message = "[CreateTable]: "
                + request.getSchemaName() + " | " + request.getTable().getName() + " | " + request.getTable().getColumnsList();
        logger.info(message);
        tableMap.put(request.getTable().getName(), request.getTable());
        responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void alterTable(AlterTableRequest request, StreamObserver<AlterTableResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();

        String message = "[AlterTable]: " +
                request.getSchemaName() + " | " + request.getTable().getName() + " | " + request.getTable().getColumnsList();
        logger.info(message);
        tableMap.put(request.getTable().getName(), request.getTable());
        responseObserver.onNext(AlterTableResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void truncate(TruncateRequest request, StreamObserver<TruncateResponse> responseObserver) {
        System.out.printf("[TruncateTable]: %s | %s | soft=%s%n",
                request.getSchemaName(), request.getTableName(), request.hasSoft());
        responseObserver.onNext(TruncateResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void writeBatch(WriteBatchRequest request, StreamObserver<WriteBatchResponse> responseObserver) {
        String message = "[WriteBatch]: " + request.getSchemaName() + " | " + request.getTable().getName();
        logger.warning(String.format("Sample severe message: %s", message));
        for (String file : request.getReplaceFilesList()) {
            System.out.println("Replace files: " + file);
        }
        for (String file : request.getUpdateFilesList()) {
            System.out.println("Update files: " + file);
        }
        for (String file : request.getDeleteFilesList()) {
            System.out.println("Delete files: " + file);
        }
        responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    /*
     *
     * Reference: https://github.com/fivetran/fivetran_sdk/blob/main/how-to-handle-history-mode-batch-files.md
     *
     * The `WriteHistoryBatch` method is used to write history mode-specific batch files to the destination.
     * The incoming batch files are processed in the exact following order:
     * 1. `earliest_start_files`
     * 2. `replace_files`
     * 3. `update_files`
     * 4. `delete_files`
     *
     * 1. **`earliest_start_files`**
     *    - Contains a single record per primary key with the earliest `_fivetran_start`.
     *    - Operations:
     *      - Delete overlapping records where `_fivetran_start` is greater than `earliest_fivetran_start`.
     *      - Update history mode-specific system columns (`fivetran_active` and `_fivetran_end`).
     *
     * 2. **`update_files`**
     *    - Contains records with modified column values.
     *    - Process:
     *      - Modified columns are updated with new values.
     *      - Unmodified columns are populated with values from the last active record in the destination.
     *      - New records are inserted while maintaining history tracking.
     *
     * 3. **`replace_files`**
     *    - Contains records where all column values are modified.
     *    - Process:
     *      - Insert new records directly into the destination table.
     *
     * 4. **`delete_files`**
     *    - Deactivates records in the destination table.
     *    - Process:
     *      - Set `_fivetran_active` to `FALSE`.
     *      - Update `_fivetran_end` to match the corresponding record’s end timestamp from the batch file.
     *
     * This structured processing ensures data consistency and historical tracking in the destination table.
     */
    @Override
    public void writeHistoryBatch(WriteHistoryBatchRequest request, StreamObserver<WriteBatchResponse> responseObserver) {
        String message = "[WriteHistoryBatch]: " + request.getSchemaName() + " | " + request.getTable().getName();
        logger.warning(String.format("Sample severe message: %s", message));
        for (String file : request.getEarliestStartFilesList()) {
            System.out.println("EarliestStart files: " + file);
        }
        for (String file : request.getReplaceFilesList()) {
            System.out.println("Replace files: " + file);
        }
        for (String file : request.getUpdateFilesList()) {
            System.out.println("Update files: " + file);
        }
        for (String file : request.getDeleteFilesList()) {
            System.out.println("Delete files: " + file);
        }
        responseObserver.onNext(WriteBatchResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }
}
