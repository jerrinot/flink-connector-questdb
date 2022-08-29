package info.jerrinot.flink.connector.questdb;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.data.RowData;

import io.questdb.client.Sender;

public class QuestDBSinkWriter implements SinkWriter<RowData> {
    private final Sender sender;
    private final QuestDBSink.WriterAdapter adapter;

    public QuestDBSinkWriter(QuestDBSink.WriterAdapter adapter, Sender sender) {
        this.adapter = adapter;
        this.sender = sender;
    }

    @Override
    public void write(RowData element, Context context) {
        Long timestamp = context.timestamp();
        adapter.write(element, timestamp);
    }

    @Override
    public void flush(boolean endOfInput) {
        sender.flush();
    }

    @Override
    public void close() {
        sender.close();
    }
}
