package DSPPCode.mapreduce.frequent_item_analysis.impl;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FrequentItemAnalysisCombinerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    // 进行合并操作
    for (IntWritable value : values) {
      sum += value.get();
    }
    // 输出合并的结果
    context.write(key, new IntWritable(sum));
  }
}
