package LetterCount;

import org.apache.hadoop.examples.SecondarySort;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IndexPair implements WritableComparable<IndexPair> {
    private String key;
    private Integer value;

    public IndexPair() {}

    public void set(String key, int value) {
        this.key = key;
        this.value = value;
    }

    public IndexPair(String key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(IndexPair indexPair) {
        int cmp = value.compareTo(indexPair.value);
        if (0 != cmp) {
            return cmp;
        }
        return key.compareTo(indexPair.key);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(key.getBytes());
        dataOutput.writeInt(value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.key = dataInput.readLine();
        this.value = dataInput.readInt();
    }

    public static class Comparator extends WritableComparator {
        public Comparator() { super(IndexPair.class); }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//            return super.compare(b1, s1, l1, b2, s2, l2);
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }

    }
}
