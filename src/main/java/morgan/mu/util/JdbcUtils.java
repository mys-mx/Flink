package morgan.mu.util;

import morgan.mu.conf.ConfigurationManager;
import morgan.mu.constant.Constants;
import com.alibaba.druid.pool.DruidDataSource;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class JdbcUtils {

    private static final Logger logger = LoggerFactory.getLogger(JdbcUtils.class);
    private static DruidDataSource dataSource;

    static {
        InputStream configStream = null;
        Properties properties = new Properties();
        try {
            configStream = JdbcUtils.class.getResourceAsStream("/pro/druid_mysql.properties");
            properties.load(configStream);
        } catch (Exception pe) {
            logger.error("### load datasource config failed ###", pe);
        } finally {
            if (configStream != null) {
                try {
                    configStream.close();
                } catch (Exception e) {
                    logger.error("### create datasource failed ###", e);
                }
            }
        }
        dataSource = new DruidDataSource();
        dataSource.configFromPropety(properties);
    }


    private static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }


    public static List queryForBeanList(String sql, Class clz, Object... params) {
        Connection conn = null;
        try {
            //打开数据库连接
            conn = getConnection();
            //jdbc连接工具，驱动.......
            QueryRunner qr = new QueryRunner();
            List list = (List) qr.query(conn, sql, new BeanListHandler(clz), params);
            return list;
        } catch (Exception e) {
            logger.error("### queryForBeanList Failed ###" + sql, e);
            return null;
        } finally {
            //关闭数据库连接
            DbUtils.closeQuietly(conn);
        }
    }

    public static List MysqlList(String sql, Class clz, Object... params) {
        Connection conn = null;
        try {
            //打开数据库连接
            conn = getConnection();
            //jdbc连接工具，驱动.......
            QueryRunner qr = new QueryRunner();
            List list = (List) qr.query(conn, sql, new BeanListHandler(clz), params);
            return list;
        } catch (Exception e) {
            logger.error("### queryForBeanList Failed ###" + sql, e);
            return null;
        } finally {
            //关闭数据库连接
            DbUtils.closeQuietly(conn);
        }
    }


    //UPDATE t_user SET name=?,nickName=?,password=?,age=?,height=? WHERE name=?;
    public static void update(String sql, Object... params) {
        Connection conn = null;
        try {
            conn = getConnection();
            QueryRunner qr = new QueryRunner();
            qr.update(conn, sql, params);
        } catch (Exception e) {
            logger.error("### updateWithParams Failed ###" + sql, e);
        } finally {
            DbUtils.closeQuietly(conn);
        }
    }


    // INSERT INTO t_user (name,nickName,password,age,height) VALUES (?,?,?,?,?);
    public static void insertOne(String sql, Object... params) {
        update(sql, params);
    }

}




