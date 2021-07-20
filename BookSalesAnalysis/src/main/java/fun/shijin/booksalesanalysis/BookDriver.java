package fun.shijin.booksalesanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author Jiaman
 * @Date 2021/7/9 11:49
 * @Desc
 */

public class BookDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, URISyntaxException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job.setJarByClass(BookDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(BookMapper.class);
        job.setReducerClass(BookReducer.class);

        // 4 设置map输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BookBean.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(BookBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 设置分区类
        job.setPartitionerClass(ChannelPartition.class);

        // 设置reduce task个数
        job.setNumReduceTasks(8);

        // 加载缓存数据
        job.addCacheFile(new URI("file:///F:/Demo/bigdata/Top10Book/src/main/resources/inputcache/product.csv"));

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("F:\\Demo\\bigdata\\Top10Book\\src\\main\\resources\\input\\order.csv"));
        FileOutputFormat.setOutputPath(job, new Path("F:\\Demo\\bigdata\\Top10Book\\src\\main\\resources\\output"));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
