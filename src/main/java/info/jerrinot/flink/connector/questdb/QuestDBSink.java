package info.jerrinot.flink.connector.questdb;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import io.questdb.client.Sender;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class QuestDBSink implements Sink<RowData> {

    private final DataType physicalRowDataType;
    private final QuestDBConfiguration questDBConfiguration;

    public QuestDBSink(DataType physicalRowDataType, QuestDBConfiguration questDBConfiguration) {
        this.physicalRowDataType = physicalRowDataType;
        this.questDBConfiguration = questDBConfiguration;
    }

    @Override
    public SinkWriter<RowData> createWriter(InitContext context) {
        Sender.LineSenderBuilder builder = Sender
                .builder()
                .address(questDBConfiguration.getHost());
        if (questDBConfiguration.isTlsEnabled()) {
            builder.enableTls();
        }
        questDBConfiguration.getToken().ifPresent(t -> {
            String username = questDBConfiguration.getUserId();
            builder.enableAuth(username).authToken(t);
        });
        questDBConfiguration.getBufferSize().ifPresent(buffer -> builder.bufferCapacity(buffer * 1024));
        Sender sender = builder.build();
        return new QuestDBSinkWriter(physicalRowDataType, questDBConfiguration.getTable(), sender);
    }
}
