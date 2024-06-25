package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class FrequentItemAnalysisReducerImpl extends FrequentItemAnalysisReducer {
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Reducer<Text, IntWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
    // 获取交易记录数
    int total = context.getConfiguration().getInt("count.of.transactions", 0);
    // 获取支持度
    double support = context.getConfiguration().getDouble("support", 0);
    // 计算阈值
    int threshold = (int) Math.ceil(total * support);

    int sum = 0;
    // 遍历累加求和
    for (IntWritable value : values) {
      sum += value.get();
    }

    // 判断是否是频繁项
    if (sum >= threshold) {
      context.write(key, NullWritable.get());
    }
  }
}
