package tableapi;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.types.Row;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
//        TableEnvironment tEnv = TableEnvironment.create(settings);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // JDBC Catalog
//        JdbcCatalog catalog = new JdbcCatalog("mysql", "world", "root", "root", "jdbc:mysql://localhost:3306");
//        tEnv.registerCatalog("mysql", catalog);
//
//        tEnv.useCatalog("mysql");
//        tEnv.useDatabase("world");
//
//        List<String> world = catalog.listTables("world");
//        System.out.println(world);
//        Table table = tEnv.from(world.get(0)).select($("ID"));
//        tEnv.toDataStream(table, Row.class).print("city");


        // ddl sql
        String city = "CREATE TABLE MyCityTable (\n" +
                "  id INT,\n" +
                "  name STRING,\n" +
                "  countrycode STRING,\n" +
                "  district STRING,\n" +
                "  population int,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://localhost:3306/world',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root',\n" +
                "   'driver' = 'com.mysql.cj.jdbc.Driver',\n" +
                "   'table-name' = 'city'\n" +
                ");";

        tEnv.executeSql(city);

        Table table1 = tEnv.sqlQuery("select id, name, countrycode, district, population from MyCityTable where id < 100");
        tEnv.toDataStream(table1, Row.class).print("sql");


        env.execute("jdbc catalog");
    }
}
