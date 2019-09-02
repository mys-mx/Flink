package morgan.mu.sink;


import morgan.mu.util.HBaseDriverManager;
import morgan.mu.util.HBaseUtil;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class
HbaseSink implements OutputFormat<Tuple2<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(HbaseSink.class);

    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private Table table = null;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        conn = HBaseDriverManager.getConnection();
        table = conn.getTable(TableName.valueOf("classes"));
    }

    @Override
    public void writeRecord(Tuple2<String, String> record) throws IOException {
        String[] field = {
                "totalBoxUnitInfo", "splitTotalBox"
        };

        String[] value = {
                record.f0, record.f1
        };
        try {
            HBaseUtil.putToHBase(table, "0001", "user", field, value);
            System.out.println("-------成功写入hbase-------");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if (table != null) {
            table.close();
        }
        if (conn != null) {
            conn.close();
        }

    }
}
