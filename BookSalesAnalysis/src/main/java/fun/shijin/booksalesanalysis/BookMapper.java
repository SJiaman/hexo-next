package fun.shijin.booksalesanalysis;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author Jiaman
 * @Date 2021/7/9 11:48
 * @Desc
 */

public class BookMapper extends Mapper<LongWritable, Text, Text, BookBean> {
    private Text k = new Text();
    private BookBean v = new BookBean();
    private Map<String, String> product = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
       // 把图书类目表缓存到product
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        // 包装成bufferedReader,方便进行按行读取
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
            // 切割每一行
            String[] world = line.split(",");
            product.put(world[0], world[1]);
        }
        // 关流
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取订单表的数据
        String[] fields = value.toString().split(",");

        String bookName = product.get(fields[3]);

        if (bookName != null && !"null".equals(bookName)) {
            v.setBookId(fields[3]);
            v.setBookName(bookName);
            v.setChannel(fields[7]);
            v.setAmount(1);
            k.set(bookName);
        }


        context.write(k, v);
    }
}
