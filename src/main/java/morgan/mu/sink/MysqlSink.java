package morgan.mu.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Title: MysqlSink
 * @Description: 写数据到mysql
 * @Author: YuSong.Mu
 * @Date: 2019/8/22 15:13
 */
public class MysqlSink extends RichSinkFunction<Tuple4<String, Integer, String, String>> {
    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void invoke(Tuple4<String, Integer, String, String> value) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式

        try {
            preparedStatement.setString(1, value.f0);
            preparedStatement.setInt(2, value.f1);
            preparedStatement.setString(3, value.f2);
            preparedStatement.setString(4, value.f3);
            preparedStatement.setString(5, df.format(new Date()));

            preparedStatement.executeUpdate();
            System.out.println("---------成功写入mysql--------");
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
        // 加载JDBC驱动5.6以下下的mysql驱动
        // 5.7及以上的mysql驱动是com.mysql.cj.jdbc.Driver
        Class.forName("com.mysql.jdbc.Driver");
        // 获取数据库连接
        connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&useSSL=true", "root", "root");//写入mysql数据库
        preparedStatement = connection.prepareStatement("replace into student(name,age,school,subject,replace_into_time) values(?,?,?,?,?) ");//insert sql在配置文件中
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (null != preparedStatement) {
            preparedStatement.close();
        }
        if (null != connection) {
            connection.close();
        }
    }


}
