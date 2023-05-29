package tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class TableTest_Hive {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
//        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        String name = "myhive";
        String defaultDatabase = "mydatabase";
        String hiveConfDir = "";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tEnv.registerCatalog("myhive", hive);

        // 设置catalog为当前的catalog
        tEnv.useCatalog("myhive");

        /**
         * 注意：可以为执行的每个语句动态切换方言。无需重新启动会话即可使用其他方言。
         */
        // 设置使用hive方言
//        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        // 设置使用default方言
//        tEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);







    }
}
