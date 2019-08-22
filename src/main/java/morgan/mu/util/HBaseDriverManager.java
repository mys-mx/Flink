package morgan.mu.util;

import morgan.mu.conf.ProConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * @Title: HBaseDriverManager
 * @Description:  hbase 工厂类
 * @Author: YuSong.Mu
 * @Date: 2019/8/14 11:51
 */
public class HBaseDriverManager {

    private static ProConfig proConfig = new ProConfig();
    public static Configuration conf = null;
    private static Connection conn;
    private static int POOL_MAX_SIEZE = 500;



    public static Configuration getHbaseConf() {
        return conf;
    }

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", proConfig.getHbaseConnection());
        conf.set("hbase.zookeeper.property.clientPort", proConfig.getHbasePort());
//        conf.set("hbase.master.port", "60000");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static HBaseAdmin getHBaseAdmin() throws IOException {
        HBaseAdmin hbaseAdmin = null;
        try {
            hbaseAdmin = (HBaseAdmin) (conn.getAdmin());
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
        return hbaseAdmin;
    }


    public static synchronized Table getHtable(String tableName)  {
        if (conn != null) {
            try {
                return conn.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                conn = ConnectionFactory.createConnection(conf);
                return conn.getTable(TableName.valueOf(tableName));
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return null;
    }

    public static Connection getConnection() {
        return conn;
    }

    public static synchronized void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        HBaseAdmin hAdmin = null;
        try {
            hAdmin = getHBaseAdmin();

            TableName[] names = hAdmin.listTableNames();
            for (TableName name : names) {
                System.out.println(name.getNameAsString());

            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                hAdmin.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
