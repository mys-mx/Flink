package morgan.mu.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.List;

/**
 * @Title: MongoUtils
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2019/8/22 15:44
 */
public class MongoUtils {
    public static MongoClient getConnect(){
        ServerAddress serverAddress = new ServerAddress("dds-2ze0fb731a87dd54-pub.mongodb.rds.aliyuncs.com", 3717);
        List<MongoCredential> credential = new ArrayList<>();
        //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
        MongoCredential mongoCredential1 = MongoCredential.createScramSha1Credential("boxoffice", "boxoffice", "AiadsMongo20190429".toCharArray());
        credential.add(mongoCredential1);
        //通过连接认证获取MongoDB连接
        MongoClient mongoClient = new MongoClient(serverAddress, credential);
        return mongoClient;
    }

}
