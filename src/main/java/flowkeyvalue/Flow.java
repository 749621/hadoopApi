package flowkeyvalue;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flow implements Writable {

    private long up;
    private long down;
    private long sum;

    public Flow() {
    }

    public long getUp() {
        return up;
    }

    public void setUp(long up) {
        this.up = up;
    }

    public long getDown() {
        return down;
    }

    public void setDown(long down) {
        this.down = down;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    //序列化
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {

        up = in.readLong();
        down = in.readLong();
        sum = in.readLong();
    }

    @Override
    public String toString() {
        return up + "\t" + down + "\t" + sum;
    }
}
