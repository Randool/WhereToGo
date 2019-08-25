package Comments;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class CommentsReducer extends Reducer<Text, NullWritable, NullWritable, Line> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // Map阶段已将评论中的“,”替换为全角“，”
        String[] items = key.toString().split(",");

        Line line = new Line();
        line.setScenery(items[0]);
        line.setComment(items[1]);
        line.setEmotion(Integer.valueOf(items[2]));
        line.setYear(Integer.valueOf(items[3]));
        line.setQuarter(Integer.valueOf(items[4]));

        context.write(NullWritable.get(), line);
    }

}
