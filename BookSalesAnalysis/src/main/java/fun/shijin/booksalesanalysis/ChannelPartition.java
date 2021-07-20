package fun.shijin.booksalesanalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Author Jiaman
 * @Date 2021/7/9 12:28
 * @Desc
 */

public class ChannelPartition extends Partitioner<Text, BookBean> {
    @Override
    public int getPartition(Text text, BookBean bookBean, int i) {

        String channel = bookBean.getChannel();

        int partition = 0;

        if ("阿里博客".equals(channel)) {
            partition = 0;
        } else if ("聚划算".equals(channel)) {
            partition = 1;
        } else if ("淘宝橱窗".equals(channel)) {
            partition = 2;
        } else if ("淘宝社区".equals(channel)) {
            partition = 3;
        } else if ("淘宝搜索".equals(channel)) {
            partition = 4;
        } else if ("淘宝直播".equals(channel)) {
            partition = 5;
        } else if ("直通车".equals(channel)) {
            partition = 6;
        }else if ("搜索".equals(channel)){
            partition = 7;
        }
        return partition;
    }
}
