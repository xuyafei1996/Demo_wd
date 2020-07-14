package MapReduce_Demo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //累加操作
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //包装结果并输出
        total.set(sum);
        context.write(key, total);
    }
}
