package info.jerrinot.flink.connector.questdb;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.table.api.Expressions.row;

@ExtendWith(TestLoggerExtension.class)
@Testcontainers
public class QuestDBDynamicSinkFactoryTest {

    @Container
    public GenericContainer questdb = new GenericContainer(DockerImageName.parse("questdb/questdb:6.5.1"))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withExposedPorts(9009);

    @Test
    public void testSmoke() throws ExecutionException, InterruptedException {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.executeSql(
                "CREATE TABLE questTable ("
                        + "a BIGINT NOT NULL,\n"
                        + "b TIMESTAMP,\n"
                        + "c STRING NOT NULL,\n"
                        + "d FLOAT,\n"
                        + "e TINYINT NOT NULL,\n"
                        + "f DATE,\n"
                        + "g TIMESTAMP NOT NULL,"
                        + "h as a + 2\n"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "questdb")
                        + String.format("'%s'='%s',\n", "host", questdb.getHost() + ":" + questdb.getMappedPort(9009))
                        + String.format("'%s'='%s'\n", "table", "flink_table")
                        + ")");

        tableEnvironment
                .fromValues(
                        row(
                                1L,
                                LocalDateTime.of(2022, 6, 1, 10, 10, 10, 10_000),
                                "ABCDE",
                                12.12f,
                                (byte) 2,
                                LocalDate.ofEpochDay(12345),
                                LocalDateTime.parse("2012-12-12T12:12:12")))
                .executeInsert("questTable")
                .await();
    }

}
