package info.jerrinot.flink.connector.questdb;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import io.questdb.client.Sender;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class QuestDBSink implements Sink<RowData> {

    private final DataType physicalRowDataType;
    private final QuestDBConfiguration questDBConfiguration;

    public QuestDBSink(DataType physicalRowDataType, QuestDBConfiguration questDBConfiguration) {
        this.physicalRowDataType = physicalRowDataType;
        this.questDBConfiguration = questDBConfiguration;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) throws IOException {
        Sender.LineSenderBuilder builder = Sender
                .builder()
                .address(questDBConfiguration.getHost());
        if (questDBConfiguration.isTlsEnabled()) {
            builder = builder.enableTls();
        }
        Optional<String> token = questDBConfiguration.getToken();
        if (token.isPresent()) {
            String username = questDBConfiguration.getUserId();
            builder = builder.enableAuth(username).authToken(token.get());
        }
        Sender sender = builder.build();
        WriterAdapter adapter = createAdapter(sender, physicalRowDataType, questDBConfiguration.getTable());
        return new QuestDBSinkWriter(adapter, sender);
    }

    private static WriterAdapter createAdapter(Sender sender, DataType physicalRowDataType, String targetTable) {
        final RowType rowType = (RowType) physicalRowDataType.getLogicalType();
        List<RowType.RowField> fields = rowType.getFields();
        return (rw, ts) -> {
            sender.table(targetTable);
            for (int i = 0; i < fields.size(); i++) {
                RowType.RowField rowField = fields.get(i);
                LogicalType type = rowField.getType();
                String name = rowField.getName();
                LogicalTypeRoot logicalTypeRoot = type.getTypeRoot();
                switch (logicalTypeRoot) {
                    case CHAR:
                    case VARCHAR:
                        sender.stringColumn(name, rw.getString(i).toString());
                        break;
                    case BOOLEAN:
                        sender.boolColumn(name, rw.getBoolean(i));
                        break;
                    case DECIMAL:
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                        sender.longColumn(name, rw.getInt(i));
                        break;
                    case FLOAT:
                        sender.doubleColumn(name, rw.getFloat(i));
                        break;
                    case DOUBLE:
                        sender.doubleColumn(name, rw.getDouble(i));
                        break;
                    case DATE:
                        LocalDate localDate = LocalDate.ofEpochDay(rw.getInt(i));
                        ZonedDateTime utc = localDate.atStartOfDay(ZoneId.of("UTC"));
                        utc.toEpochSecond();
                        long micros = TimeUnit.SECONDS.toMicros(utc.toEpochSecond());
                        sender.timestampColumn(name, micros);
                        break;
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                    case TIMESTAMP_WITH_TIME_ZONE:
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        final int timestampPrecision = ((TimestampType) type).getPrecision();
                        TimestampData timestamp = rw.getTimestamp(i, timestampPrecision);
                        micros = TimeUnit.MILLISECONDS.toMicros(timestamp.getMillisecond());
                        long microsInMillis = TimeUnit.NANOSECONDS.toMicros(timestamp.getNanoOfMillisecond());
                        sender.timestampColumn(name, micros + microsInMillis);
                        break;
                    case TIME_WITHOUT_TIME_ZONE:
                        long l = rw.getInt(i);
                        sender.longColumn(name, l);
                        break;
                    case INTERVAL_YEAR_MONTH:
                    case INTERVAL_DAY_TIME:
                    case ARRAY:
                    case MULTISET:
                    case MAP:
                    case ROW:
                    case DISTINCT_TYPE:
                    case STRUCTURED_TYPE:
                    case NULL:
                    case RAW:
                    case SYMBOL:
                    case UNRESOLVED:
                    case BINARY:
                    case VARBINARY:
                    default:
                        throw new UnsupportedOperationException(logicalTypeRoot + " type not supported");
                }
            }
            if (ts == null) {
                sender.atNow();
            } else {
                sender.at(TimeUnit.MILLISECONDS.toNanos(ts));
            }
        };

    }

    @FunctionalInterface
    public interface WriterAdapter {
        void write(RowData rowData, Long timestamp);
    }
}
