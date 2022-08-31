package info.jerrinot.flink.connector.questdb;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.types.DataType;

/**
 *
 *
 */
public final class QuestDBDynamicTableSink implements DynamicTableSink {
    private final DataType physicalRowDataType;
    private final QuestDBConfiguration questDBConfiguration;

    public QuestDBDynamicTableSink(DataType physicalRowDataType, QuestDBConfiguration questDBConfiguration) {
        this.physicalRowDataType = physicalRowDataType;
        this.questDBConfiguration = questDBConfiguration;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkV2Provider.of(new QuestDBSink(physicalRowDataType, questDBConfiguration));
    }

    @Override
    public DynamicTableSink copy() {
        return new QuestDBDynamicTableSink(physicalRowDataType, questDBConfiguration);
    }

    @Override
    public String asSummaryString() {
        return QuestDBDynamicSinkFactory.FACTORY_IDENTIFIER;
    }
}
