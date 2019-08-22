package morgan.mu.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @Title: HbaseUtil
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2019/8/14 12:58
 */
public class HBaseUtil {

    /**
     * @param tableName
     * @param columFamilys 列簇名称
     * @throws IOException
     */
    public static void createTable(String tableName, String... columFamilys) throws IOException {
        if (StringUtils.isBlank(tableName) || columFamilys.length == 0) {
            return;
        }
        HBaseAdmin hAmin = HBaseDriverManager.getHBaseAdmin();
        HTableDescriptor hd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columFamilys) {
            if (!StringUtils.isBlank(tableName)) {
                hd.addFamily(new HColumnDescriptor(cf));
            }
        }
        try {
            hAmin.createTable(hd);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            hAmin.close();
        }
    }


    // 禁用表
    public void disableTable(HBaseAdmin admin, String table) {
        try {
            admin.disableTable(table);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // 删除表
    public void dropTable(HBaseAdmin admin, String tableName) {
        if (existsTable(admin, tableName)) {
            disableTable(admin, tableName);
            try {
                admin.deleteTable(tableName);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    // 判定表是否存在
    public static boolean existsTable(HBaseAdmin admin, String tableName) {
        try {
            return admin.tableExists(tableName.getBytes());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();

            return false;
        }
    }


    // 插入hbase中获得数据，傳入表名tableName,行键rowkey,列族cf,列名column,值value.
    public static String getValue(Table table, String rowkey,
                                  String cf, String column) {
        Get get = new Get(rowkey.getBytes());
        get.addColumn(cf.getBytes(), column.getBytes());
        String val = null;
        try {
            Result result = table.get(get);
            if (result.value() != null) {
                val = new String(result.value());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return val;
    }

    //通过传入rowkey和cf，获得该rowkey在cf下的所有值
    public static Result getValues(Table htable, byte[] rowkey,
                                   String cf) {
        Result result = null;
        Get get = new Get(rowkey);
        get.addFamily(cf.getBytes());
        try {
            result = htable.get(get);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }

    /**
     * 插入數據到Hbase中，傳入表名tableName,行键rowkey,列族cf,列名column,值value.
     *
     * @param table
     * @param rowkey
     * @param cf
     * @param column
     * @param value  column和value的成员个数要相同
     * @throws Exception
     */
    public static void putToHBase(Table table, String rowkey,
                                  String cf, String[] column, String[] value) throws Exception {
        if (null == column && null == value) {
            throw new Exception("column OR value invalid");
        }
        if (column.length != value.length) {
            throw new Exception("column.lenth must equals value.lenth");
        }
        Put put = new Put(rowkey.getBytes());
        for (int i = 0; i < column.length; i++) {
            put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column[i]), Bytes.toBytes(value[i]));
        }
        try {
            table.put(put);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    //传入单个put
    public static void putToHBase(Table htable, Put put) {
        if (put != null) {
            try {
                htable.put(put);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }


    //传入List<Put>，批写入。建议使用这种方法
    public static void putToHBase(Table htable, List<Put> puts) {
        if (!puts.isEmpty() && puts.size() > 0) {
            try {
                htable.put(puts);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    //删除hbase中rokey下某个列族的所有值
    public static void deleteRowkeyByCF(Table htable, String rowkey, String cf) throws IOException {
        Delete delete = new Delete(rowkey.getBytes());
        delete.addFamily(cf.getBytes());
        htable.delete(delete);
    }


    // 查询hbase中获得数据，傳入表名tableName,行键rowkey,列族cf,列名column,值value.
    public static String getValue(Table htable, String rowkey,
                                  byte[] cf, String column) {
        Get get = new Get(rowkey.getBytes());
        get.addColumn(cf, column.getBytes());
        String val = null;
        try {
            Result result = htable.get(get);
            if (result.value() != null) {
                val = new String(result.value());
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return val;
    }


    public static Result getValues(Table table, String rowkey, String cf) throws IOException {
        Result result = null;
        Get get = new Get(Bytes.toBytes(rowkey));
        get.setMaxVersions(5);
        get.addFamily(Bytes.toBytes(cf));

        try {
            result = table.get(get);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return result;
    }


    public static void main(String[] args) {
        HTable table = (HTable) HBaseDriverManager.getHtable("gold_spot");
        String value = HBaseUtil.getValue(table, "fe5cebc4d8a8ca01d56081ca817fc8df", "spot", "mid");
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(value);
        HTable table1 = (HTable) HBaseDriverManager.getHtable("gold_spot");
        String value1 = HBaseUtil.getValue(table, "fe5cebc4d8a8ca01d56081ca817fc8df", "spot", "mid");
        System.out.println(value1);
    }
}


