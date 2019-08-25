package Comments;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.*;


public class CommentsDriver {

    public static void main(String[] args) throws Exception {
        // 创建配置对象，此对象是根据hadoop的core-site.xml, hdfs-site.xml配置创建
        Configuration conf = new Configuration();

        // 读取配置文件
        InputStream inputStream = new FileInputStream("src/main/java/config.json");
        String text = IOUtils.toString(inputStream, "utf8");
        JSONObject json = (JSONObject) JSONObject.parse(text);

        DBConfiguration.configureDB(conf,
                json.getString("jdbc_driver"), json.getString("db_url"),
                json.getString("username"), json.getString("password"));

        Job job = Job.getInstance(conf, "Comments");

        job.setJarByClass(CommentsDriver.class);
        job.setMapperClass(CommentsMapper.class);
        job.setReducerClass(CommentsReducer.class);

        // Mapper输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Path inputPath, outputPath;
        if (json.getBoolean("cloud")) {
            // 云端路径
            inputPath = new Path(json.getString("cloud_file"));
            outputPath = new Path("/output");
        } else {
            // 使用本地路径
            inputPath = new Path(json.getString("local_file"));
            outputPath = new Path("output");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
        }

        job.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job, "Comments", "scenery", "comment", "emotion", "year", "quarter");

        // 添加输入和输出路径到计算程序中
        FileInputFormat.addInputPath(job, inputPath);
        if (!json.getBoolean("cloud")) {
            FileOutputFormat.setOutputPath(job, outputPath);
        }

        // 等待MapReduce计算完成后退出应用程序
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
