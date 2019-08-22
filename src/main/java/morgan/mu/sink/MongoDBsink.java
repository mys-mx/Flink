package morgan.mu.sink;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import morgan.mu.util.MongoUtils;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * @Title: MongoDBsink
 * @Description: java类作用描述
 * @Author: YuSong.Mu
 * @Date: 2019/8/22 15:13
 */
public class MongoDBsink extends RichSinkFunction<Tuple5<String, String, String, String, String>> {

    private static final long serialVersionUID = 1L;
    MongoClient mongoClient = null;

    public void invoke(Tuple5<String, String, String, String, String> value) {
        try {
            if (mongoClient != null) {
                mongoClient = MongoUtils.getConnect();
                MongoDatabase db = mongoClient.getDatabase("boxoffice");
                MongoCollection collection = db.getCollection("test1");
                List<Document> list = new ArrayList<>();
                Document doc = new Document();
                doc.append("IP", value.f0);
                doc.append("TIME", value.f1);
                doc.append("CourseID", value.f2);
                doc.append("Status_Code", value.f3);
                doc.append("Referer", value.f4);
                list.add(doc);
                System.out.println("Insert Starting");
                collection.insertMany(list);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void open(Configuration parms) throws Exception {
        super.open(parms);
        mongoClient = MongoUtils.getConnect();
    }

    public void close() throws Exception {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }


}
