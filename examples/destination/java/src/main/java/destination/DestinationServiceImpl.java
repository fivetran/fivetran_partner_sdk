package destination;

import com.google.common.collect.Lists;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.Timestamp;
import fivetran_sdk.v2.*;
import io.grpc.stub.StreamObserver;
import java.time.Instant;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class DestinationServiceImpl extends DestinationConnectorGrpc.DestinationConnectorImplBase {
    @Override
    public void configurationForm(ConfigurationFormRequest request, StreamObserver<ConfigurationFormResponse> responseObserver) {
        responseObserver.onNext(
                ConfigurationFormResponse.newBuilder()
                        .setSchemaSelectionSupported(true)
                        .setTableSelectionSupported(true)
                        .addAllFields(Arrays.asList(
                                FormField.newBuilder()
                                        .setSingle(Field.newBuilder().setName("host").setLabel("Host").setPlaceholder("my.example.host")
                                                .setRequired(true).setTextField(TextField.PlainText).build())
                                        .build(),
                                FormField.newBuilder()
                                        .setSingle(Field.newBuilder().setName("password").setLabel("Password").setPlaceholder("p4ssw0rd")
                                                .setRequired(true).setTextField(TextField.Password).build())
                                        .build(),
                                FormField.newBuilder()
                                        .setSingle(Field.newBuilder().setName("region").setLabel("AWS Region").setDefaultValue("US-EAST").setRequired(false)
                                                .setDropdownField(DropdownField.newBuilder().addAllDropdownField(Arrays.asList("US-EAST", "US-WEST")).build())
                                                .build())
                                        .build(),
                                FormField.newBuilder()
                                        .setSingle(Field.newBuilder().setName("hidden").setLabel("my-hidden-value").setTextField(TextField.Hidden).build())
                                        .build(),
                                FormField.newBuilder()
                                        .setSingle(Field.newBuilder().setName("isPublic").setLabel("Public?").setDescription("Is this public?")
                                                .setToggleField(ToggleField.newBuilder().build())
                                                .build())
                                        .build(),
                                FormField.newBuilder().setFieldSet(
                                        FieldSet.newBuilder()
                                                .addAllFields(Arrays.asList(
                                                        FormField.newBuilder().setSingle(
                                                                        Field.newBuilder()
                                                                                .setName("ssh_tunnel_host")
                                                                                .setLabel("SSH Host")
                                                                                .setTextField(TextField.PlainText)
                                                                                .setPlaceholder("127.0.0")
                                                                                .setRequired(true)
                                                                                .build())
                                                                .build(),
                                                        FormField.newBuilder().setSingle(
                                                                        Field.newBuilder()
                                                                                .setName("ssh_tunnel_user")
                                                                                .setLabel("SSH User")
                                                                                .setTextField(TextField.PlainText)
                                                                                .setPlaceholder("user_name")
                                                                                .setRequired(false)
                                                                                .build())
                                                                .build()
                                                ))
                                                .setCondition(VisibilityCondition.newBuilder()
                                                        .setFieldName("isPublic")
                                                        .setHasStringValue("false")
                                                        .build())
                                                .build())
                                        .build()
                        )).addAllTests(Arrays.asList(
                                ConfigurationTest.newBuilder().setName("connect").setLabel("Tests connection").build(),
                                ConfigurationTest.newBuilder().setName("select").setLabel("Tests selection").build()))
                        .build());

        responseObserver.onCompleted();
    }

    @Override
    public void capabilities(CapabilitiesRequest request, StreamObserver<CapabilitiesResponse> responseObserver) {
        final int maxStringLength = 1_000_000;
        final int maxBinaryLength = 1_000_000;
        final Instant maxInstant = Instant.ofEpochMilli(Long.MAX_VALUE);
        final Timestamp maxTimestamp = Timestamp.newBuilder().setSeconds(maxInstant.getEpochSecond()).setNanos(maxInstant.getNano()).build();
        final DecimalParams maxFloatDecimalParams = DecimalParams.newBuilder().setPrecision(16).setScale(16).build();
        final DecimalParams maxDoubleDecimalParams = DecimalParams.newBuilder().setPrecision(32).setScale(16).build();

        responseObserver.onNext(
                CapabilitiesResponse.newBuilder()
                        .setSupportsHistoryMode(false)
                        .addAllDataTypeMappings(Lists.newArrayList(
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.UNSPECIFIED).setMapTo(DestinationType.newBuilder().setName("UNKNOWN").setMapTo(DataType.UNSPECIFIED).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.BOOLEAN).setMapTo(DestinationType.newBuilder().setName("BOOL").setMapTo(DataType.BOOLEAN).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.SHORT).setMapTo(DestinationType.newBuilder().setName("INTEGER").setMapTo(DataType.SHORT).setMaxValue(MaxValue.newBuilder().setNumericParam(Integer.MAX_VALUE)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.INT).setMapTo(DestinationType.newBuilder().setName("INTEGER").setMapTo(DataType.INT).setMaxValue(MaxValue.newBuilder().setNumericParam(Integer.MAX_VALUE)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.LONG).setMapTo(DestinationType.newBuilder().setName("LONG_INTEGER").setMapTo(DataType.LONG).setMaxValue(MaxValue.newBuilder().setNumericParam(Integer.MAX_VALUE)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.DECIMAL).setMapTo(DestinationType.newBuilder().setName("FLOAT").setMapTo(DataType.DECIMAL).setMaxValue(MaxValue.newBuilder().setDecimalParam(maxFloatDecimalParams)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.FLOAT).setMapTo(DestinationType.newBuilder().setName("FLOAT").setMapTo(DataType.FLOAT).setMaxValue(MaxValue.newBuilder().setDecimalParam(maxFloatDecimalParams)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.DOUBLE).setMapTo(DestinationType.newBuilder().setName("DOUBLE").setMapTo(DataType.DOUBLE).setMaxValue(MaxValue.newBuilder().setDecimalParam(maxDoubleDecimalParams)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.NAIVE_TIME).setMapTo(DestinationType.newBuilder().setName("DATETIME").setMapTo(DataType.NAIVE_TIME).setMaxValue(MaxValue.newBuilder().setDateParam(maxTimestamp)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.NAIVE_DATE).setMapTo(DestinationType.newBuilder().setName("DATE").setMapTo(DataType.NAIVE_DATE).setMaxValue(MaxValue.newBuilder().setDateParam(maxTimestamp)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.NAIVE_DATETIME).setMapTo(DestinationType.newBuilder().setName("DATETIME").setMapTo(DataType.NAIVE_DATETIME).setMaxValue(MaxValue.newBuilder().setDateParam(maxTimestamp)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.UTC_DATETIME).setMapTo(DestinationType.newBuilder().setName("DATETIME").setMapTo(DataType.UTC_DATETIME).setMaxValue(MaxValue.newBuilder().setDateParam(maxTimestamp)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.BINARY).setMapTo(DestinationType.newBuilder().setName("BLOB").setMapTo(DataType.BINARY).setMaxValue(MaxValue.newBuilder().setNumericParam(maxBinaryLength)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.XML).setUnsupported(true).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.STRING).setMapTo(DestinationType.newBuilder().setName("VARCHAR").setMapTo(DataType.STRING).setMaxValue(MaxValue.newBuilder().setNumericParam(maxStringLength)).build()).build(),
                                DataTypeMappingEntry.newBuilder().setFivetranType(DataType.JSON).setMapTo(DestinationType.newBuilder().setName("OBJECT").setMapTo(DataType.JSON).setMaxValue(MaxValue.newBuilder().setNumericParam(maxStringLength)).build()).build()))
                        .build());

        responseObserver.onCompleted();
    }

    @Override
    public void test(TestRequest request, StreamObserver<TestResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();
        String testName = request.getName();
        System.out.println("test name: " + testName);

        responseObserver.onNext(TestResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void describeTable(DescribeTableRequest request, StreamObserver<DescribeTableResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();

        DescribeTableResponse response = DescribeTableResponse.newBuilder()
                .setTable(
                        Table.newBuilder()
                                .setName(request.getTableName())
                                .addAllColumns(
                                Arrays.asList(
                                        Column.newBuilder().setName("a1").setType(DataType.UNSPECIFIED).setPrimaryKey(true).build(),
                                        Column.newBuilder().setName("a2").setType(DataType.DOUBLE).build())
                        ).build()).build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void createTable(CreateTableRequest request, StreamObserver<CreateTableResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();

        System.out.println("[CreateTable]: "
                + request.getSchemaName() + " | " + request.getTable().getName() + " | " + request.getTable().getColumnsList());
        responseObserver.onNext(CreateTableResponse.newBuilder().setSuccess(true).build());
        responseObserver.onCompleted();
    }

    @Override
    public void alterTable(AlterTableRequest request, StreamObserver<AlterTableResponse> responseObserver) {
        Map<String, String> configuration = request.getConfigurationMap();

        System.out.println("[AlterTable]: " +
                request.getSchemaName() + " | " + request.getTableName() + " | " +
                request.getChangesList().stream().map(AbstractMessage::toString).collect(Collectors.joining(", ")));
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
        System.out.println("[WriteBatch]: " + request.getSchemaName() + " | " + request.getTable().getName());
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
