package flowkeyvalue;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {

    private String phone;
    private int up;
    private int down;
    private int sum;


    public MyKey() {
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getUp() {
        return up;
    }

    public void setUp(int up) {
        this.up = up;
    }

    public int getDown() {
        return down;
    }

    public void setDown(int down) {
        this.down = down;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        phone = in.readUTF();
        up = in.readInt();
        down = in.readInt();
        sum = in.readInt();

    }

    @Override
    public int compareTo(MyKey o) {

        int result = this.sum - o.sum;
        return result!=0 ? result : this.phone.compareTo(o.phone);
    }

    @Override
    public String toString() {
        return  phone+"\t"+sum+"\t"+up+"\t"+down;
    }
}
