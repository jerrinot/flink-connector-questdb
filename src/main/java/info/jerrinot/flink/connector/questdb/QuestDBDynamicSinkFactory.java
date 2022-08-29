package info.jerrinot.flink.connector.questdb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 *
 *
 */
public final class QuestDBDynamicSinkFactory implements DynamicTableSinkFactory {
    public static final String FACTORY_IDENTIFIER = "questdb";

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig readableConfig = helper.getOptions();
        FactoryUtil.validateFactoryOptions(requiredOptions(), optionalOptions(), readableConfig);
        QuestDBConfiguration configuration = new QuestDBConfiguration(readableConfig, context.getObjectIdentifier().getObjectName());
        DataType physicalRowDataType = context.getPhysicalRowDataType();
        return new QuestDBDynamicTableSink(physicalRowDataType, configuration);
    }

    @Override
    public String factoryIdentifier() {
        return FACTORY_IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(QuestDBConfiguration.HOST);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(QuestDBConfiguration.USERNAME);
        options.add(QuestDBConfiguration.TOKEN);
        options.add(QuestDBConfiguration.TLS);
        options.add(QuestDBConfiguration.TABLE);
        return options;
    }
}
