package sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyKey implements WritableComparable<MyKey> {

    private String name;
    private int salary;
    private int age;

    public MyKey() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }



    //序列化
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(salary);
        out.writeInt(age);
        out.writeUTF(name);


    }

    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {

        salary = in.readInt();
        age = in.readInt();
        name = in.readUTF();

    }

    @Override
    public int compareTo(MyKey o) {

        int result = salary - o.salary;
        if(result != 0){

            return  result;
        }
        result = age - o.age;
        return result==0?name.compareTo(o.name):result;
    }

    @Override
    public String toString() {
        return name + "\t" +salary +"\t" + age;
    }
}
