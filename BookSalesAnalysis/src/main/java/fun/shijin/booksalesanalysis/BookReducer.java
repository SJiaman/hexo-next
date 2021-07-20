package fun.shijin.booksalesanalysis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @Author Jiaman
 * @Date 2021/7/9 11:49
 * @Desc
 */

public class BookReducer extends Reducer<Text, BookBean, BookBean, NullWritable> {
    private BookBean k = new BookBean();

    @Override
    protected void reduce(Text key, Iterable<BookBean> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (BookBean value: values) {
            sum += value.getAmount();
            k.setBookId(value.getBookId());
            k.setBookName(value.getBookName());
            k.setChannel(value.getChannel());
        }

        k.setAmount(sum);


        context.write(k, NullWritable.get());
    }
}
