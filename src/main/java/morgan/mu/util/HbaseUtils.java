
package morgan.mu.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * hbase工具类
 */
public class HbaseUtils {

    public static Configuration conf = null;
    public static Connection connection = null;
    public static Admin admin = null;



    /**
     * @desc 取得连接
     */
    public static void setConf(String quorum, String port) {
        try {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", quorum);//zookeeper地址
            conf.set("hbase.zookeeper.property.clientPort", port);
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @desc 连关闭接
     */
    public static void close() {
        try {
            if (connection != null) {
                connection.close();
            }
            if (admin != null) {
                admin.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @desc 创建表
     */
    public static void createTable(String tableName, String columnFamily) {
        try {
            TableName tbName = TableName.valueOf(tableName);
            if (!admin.tableExists(tbName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tbName);
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 添加多条记录
     */
    public static void addMoreRecord(String tableName, String family, String qualifier, List<String> rowList, String value) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<>();
            Put put = null;
            for (int i = 0; i < rowList.size(); i++) {
                put = new Put(Bytes.toBytes(rowList.get(i)));
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
                puts.add(put);
            }
            table.put(puts);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * @desc 查询rowkey下某一列值
     */
    public static String getValue(String tableName, String rowKey, String family, String qualifier) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            //返回指定列族、列名，避免rowKey下所有数据
            get.addColumn(family.getBytes(), qualifier.getBytes());
            Result rs = table.get(get);
            Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier.getBytes());
            String value = null;
            if (cell != null) {
                value = Bytes.toString(CellUtil.cloneValue(cell));
            }
            return value;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * @desc 查询rowkey下多列值，一起返回
     */
    public String[] getQualifierValue(String tableName, String rowKey, String family, String[] qualifier) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            //返回指定列族、列名，避免rowKey下所有数据
            get.addColumn(family.getBytes(), qualifier[0].getBytes());
            get.addColumn(family.getBytes(), qualifier[1].getBytes());
            Result rs = table.get(get);
            // 返回最新版本的Cell对象
            Cell cell = rs.getColumnLatestCell(family.getBytes(), qualifier[0].getBytes());
            Cell cell1 = rs.getColumnLatestCell(family.getBytes(), qualifier[1].getBytes());
            String[] value = new String[qualifier.length];
            if (cell != null) {
                value[0] = Bytes.toString(CellUtil.cloneValue(cell));
                value[1] = Bytes.toString(CellUtil.cloneValue(cell1));
            }
            return value;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    /**
     * @desc 获取一行数据
     */
    public static List<Cell> getRowCells(String tableName, String rowKey, String family) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            get.addFamily(family.getBytes());
            Result rs = table.get(get);
            List<Cell> cellList = rs.listCells();
//    		如果需要,遍历cellList
//    		if (cellList!=null) {
//    			String qualifier = null;
//    			String value = null;
//    			for (Cell cell : cellList) {
//    				qualifier = Bytes.toString( cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength());
//    				value = Bytes.toString( cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
//    				System.out.println(qualifier+"--"+value);
//    			}
//			}
            return cellList;
        } catch (IOException e) {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return null;
    }

    /**
     * 全表扫描
     *
     * @param tableName
     * @return
     */
    public static ResultScanner scan(String tableName, String family, String qualifier) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner rs = table.getScanner(scan);
//			一般返回ResultScanner，遍历即可
//			if (rs!=null){
//				String row = null;
//				String quali = null;
//    			String value = null;
//				for (Result result : rs) {
//					row = Bytes.toString(CellUtil.cloneRow(result.getColumnLatestCell(family.getBytes(), qualifier.getBytes())));
//					quali =Bytes.toString(CellUtil.cloneQualifier(result.getColumnLatestCell(family.getBytes(), qualifier.getBytes())));
//					value =Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(family.getBytes(), qualifier.getBytes())));
//					System.out.println(row+"-"+quali+"-"+value);
//				}
//			}
            return rs;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
