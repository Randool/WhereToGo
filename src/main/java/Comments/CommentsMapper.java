package Comments;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class CommentsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private String TimeStamp2Date(String timestamp) {
        long time = Integer.valueOf(timestamp);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy,MM");  // yyyy-MM-dd HH:mm:ss
        Date date = new Date(time * 1000);
        return dateFormat.format(date);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // 切分地点和评论
        int comma1 = line.indexOf(',');
        int comma2 = line.lastIndexOf(',', line.lastIndexOf(',') - 1);

        String addr = line.substring(0, comma1);
        String comm = line.substring(comma1 + 1, comma2);
        comm = comm.replaceAll(",", "，");   // 替换“,”后容易风分割数据

        // 情感分析
        int level = Emotion.sentimentClassify(comm);

        // 切分时间
        String date = TimeStamp2Date(line.substring(line.lastIndexOf(',') + 1));
        String year = date.substring(0, 4);
        Integer month = Integer.valueOf(date.substring(5));
        if (month <= 3) {
            year += ",1";
        } else if (month <= 6) {
            year += ",2";
        } else if (month <= 9) {
            year += ",3";
        } else {
            year += ",4";
        }

        // 设置键值
        String key_text = String.format("%s,%s,%d,%s", addr, comm, level, year);
        Text text = new Text();
        text.set(key_text);
        context.write(text, NullWritable.get());
    }

}
