package fun.shijin.booksalesanalysis;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author Jiaman
 * @Date 2021/7/9 11:42
 * @Desc
 */

public class BookBean implements WritableComparable<BookBean> {
    private String bookId;
    private String bookName;
    private String channel;
    private long amount;


    public BookBean() {
    }

    public BookBean(String bookId, String bookName, String channel, long amount) {
        this.bookId = bookId;
        this.bookName = bookName;
        this.channel = channel;
        this.amount = amount;
    }

    public String getBookId() {
        return bookId;
    }

    public void setBookId(String bookId) {
        this.bookId = bookId;
    }

    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return  channel + "\t" + bookName + "\t" + amount ;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.bookId);
        out.writeUTF(this.bookName);
        out.writeUTF(this.channel);
        out.writeLong(this.amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.bookId = in.readUTF();
        this.bookName = in.readUTF();
        this.channel = in.readUTF();
        this.amount = in.readLong();
    }


    @Override
    public int compareTo(BookBean o) {
        if (this.getAmount() > o.getAmount()) {
            return -1;
        } else if ((this.getAmount() < o.getAmount()) ){
            return 1;
        } else {
            return 0;
        }
    }
}
