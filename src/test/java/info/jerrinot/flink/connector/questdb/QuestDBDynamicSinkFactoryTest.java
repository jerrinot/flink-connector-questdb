package info.jerrinot.flink.connector.questdb;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.net.URLEncoder;
import java.time.LocalDate;
import java.time.LocalDateTime;

import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.row;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(TestLoggerExtension.class)
@Testcontainers
public class QuestDBDynamicSinkFactoryTest {
    private static final boolean USE_LOCAL_QUEST = false;

    private static final int ILP_PORT = 9009;
    private static final int HTTP_PORT = 9000;

    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createDefault();

    @AfterClass
    public static void classTearDown() throws IOException {
        HTTP_CLIENT.close();
    }

    @Container
    public GenericContainer questdb = new GenericContainer(DockerImageName.parse("questdb/questdb:6.5.1"))
            .withEnv("QDB_CAIRO_COMMIT_LAG", "100")
            .withEnv("JAVA_OPTS", "-Djava.locale.providers=JRE,SPI") // this makes QuestDB container much faster to start
            .withExposedPorts(ILP_PORT, HTTP_PORT);

    @Test
    public void testSmoke() throws Exception {
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
                        + String.format("'%s'='%s',\n", "host", getIlpHostAndPort())
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

        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            assertSql("\"a\",\"b\",\"c\",\"d\",\"e\",\"f\",\"g\"\r\n"
                            + "1,\"2022-06-01T10:10:10.000010Z\",\"ABCDE\",12.119999885559,2,\"2003-10-20T00:00:00.000000Z\",\"2012-12-12T12:12:12.000000Z\"\r\n",
                    "select a, b, c, d, e, f, g from flink_table");
            return true;
        });
    }

    @Test
    public void testAllSupportedTypes() throws Exception {
        TableEnvironment tableEnvironment =
                TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        tableEnvironment.getConfig().set(TableConfigOptions.LOCAL_TIME_ZONE, "UTC");

        tableEnvironment.executeSql(
                "CREATE TABLE questTable ("
                        + "a CHAR,\n"
                        + "b VARCHAR,\n"
                        + "c STRING,\n"
                        + "d BOOLEAN,\n"
//                        + "e BINARY,\n"
//                        + "f VARBINARY,\n"
//                        + "g BYTES,\n"
                        + "h DECIMAL,\n"
                        + "i TINYINT,\n"
                        + "j SMALLINT,\n"
                        + "k INTEGER,\n"
                        + "l BIGINT,\n"
                        + "m FLOAT,\n"
                        + "n DOUBLE,\n"
                        + "o DATE,"
                        + "p TIME,"
                        + "q TIMESTAMP,"
                        + "r TIMESTAMP_LTZ"
//                        + "s INTERVAL"
                        + ")\n"
                        + "WITH (\n"
                        + String.format("'%s'='%s',\n", "connector", "questdb")
                        + String.format("'%s'='%s',\n", "host", getIlpHostAndPort())
                        + String.format("'%s'='%s'\n", "table", "flink_table")
                        + ")");

        tableEnvironment
                .fromValues(
                        row("c",
                                "varchar",
                                "string",
                                true,
                                42,
                                (byte)42,
                                (short)42,
                                42,
                                10_000_000_000L,
                                42.42f,
                                42.42,
                                LocalDate.of(2022, 6, 6),
                                LocalTime.of(12, 12),
                                LocalDateTime.of(2022, 9, 3, 12, 12, 12),
                                LocalDateTime.of(2022, 9, 3, 12, 12, 12)
                        )
                ).executeInsert("questTable")
                .await();

        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            assertSql("\"a\",\"b\",\"c\",\"d\",\"h\",\"i\",\"j\",\"k\",\"l\",\"m\",\"n\",\"o\",\"p\",\"q\",\"r\"\r\n"
                            + "\"c\",\"varchar\",\"string\",true,42,42,42,42,10000000000,42.419998168945,42.42,\"2022-06-06T00:00:00.000000Z\",43920000,\"2022-09-03T12:12:12.000000Z\",\"2022-09-03T12:12:12.000000Z\"\r\n",
                    "select a, b, c, d, h, i, j, k, l, m, n, o, p, q, r from flink_table");
            return true;
        });
    }


    private void assertSql(String expectedResult, String query) throws IOException {
        String encodedQuery = URLEncoder.encode(query, "UTF-8");
        HttpGet httpGet = new HttpGet(String.format("http://%s:%d//exp?query=%s", questdb.getHost(), questdb.getMappedPort(HTTP_PORT), encodedQuery));
        CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            String s = EntityUtils.toString(entity);
            assertEquals(expectedResult, s);
        } else {
            fail("no response");
        }
    }

    private String getIlpHostAndPort() {
        if (USE_LOCAL_QUEST) {
            return "localhost:9009";
        }
        return questdb.getHost() + ":"  + questdb.getMappedPort(ILP_PORT);
    }

}
