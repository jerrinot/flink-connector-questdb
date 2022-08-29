package info.jerrinot.flink.connector.questdb;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;


/**
 *
 *
 */
public final class QuestDBSinkRuntimeProvider implements SinkV2Provider {
    private final DataType physicalRowDataType;
    private final QuestDBConfiguration questDBConfiguration;

    public QuestDBSinkRuntimeProvider(DataType physicalRowDataType, QuestDBConfiguration questDBConfiguration) {
        this.physicalRowDataType = physicalRowDataType;
        this.questDBConfiguration = questDBConfiguration;
    }

    @Override
    public Sink<RowData> createSink() {
        return new QuestDBSink(physicalRowDataType, questDBConfiguration);
    }
}
