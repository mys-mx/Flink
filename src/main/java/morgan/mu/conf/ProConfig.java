package morgan.mu.conf;

import morgan.mu.util.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * @Title: ProConfig
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2019/2/28 16:01
 */
public class ProConfig implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ProConfig.class);

    private static Properties properties = null;

    static {
        InputStream configStream = null;
        properties = new Properties();
        try {
            configStream = JdbcUtils.class.getResourceAsStream("/pro/pro.properties");
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
    }

    /**
     * hbase连接
     */
    private String hbaseConnection = properties.getProperty("hbase.connection");
    private String hbasePort = properties.getProperty("hbase.port");

    public String getHbaseConnection() {
        return hbaseConnection;
    }

    public String getHbasePort() {
        return hbasePort;
    }

    /**
     * mongo连接
     */
    private String url = properties.getProperty("mongodb.url");
    private String port = properties.getProperty("mongodb.port");
    private String user = properties.getProperty("mongodb.user");
    private String password = properties.getProperty("mongodb.password");
    private String source = properties.getProperty("mongodb.source");
    private String databaseName = properties.getProperty("mongodb.databaseName");
    private String collection = properties.getProperty("mongodb.collection");

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /**
     * kafka连接
     */



}
