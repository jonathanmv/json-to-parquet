package jonathanmv.json_to_parquet;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.simple.parser.ParseException;

import jonathanmv.json_to_parquet.model.FriendsEdge;

public class JsonToThriftMapper extends Mapper<Object, Text, Void, FriendsEdge> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String jsonString = value.toString();
        try {
            JsonToThriftConverter converter = new JsonToThriftConverter(jsonString);
            FriendsEdge thriftObject = converter.convert();

            context.write(null, thriftObject);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}