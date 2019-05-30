package group;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Mykey implements WritableComparable<Mykey> {

    private String name;
    private double salary;

    public Mykey() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }





    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(name);
        out.writeDouble(salary);
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        name = in.readUTF();
        salary = in.readDouble();
    }

    @Override
    public int compareTo(Mykey o) {

        double result = this.salary - o.salary;
        if(result != 0){
            if (result < 0){

                return -1;
            }else {

                return 1;
            }

        }
        return this.name.compareTo(o.name);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Mykey mykey = (Mykey) o;
        return Objects.equals(name, mykey.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
