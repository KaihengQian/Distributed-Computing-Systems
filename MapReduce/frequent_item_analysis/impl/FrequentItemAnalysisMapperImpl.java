package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisMapper;
import DSPPCode.mapreduce.frequent_item_analysis.question.SortHelper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class FrequentItemAnalysisMapperImpl extends FrequentItemAnalysisMapper {

  private static final IntWritable ONE = new IntWritable(1);

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 获取阶数
    int n = context.getConfiguration().getInt("number.of.pairs", 1);

    // 以英文逗号","为分隔符进行切分
    String[] words = value.toString().split(",");

    // 分解为n阶项集
    // 一阶项集的情况
    if (n == 1) {
      for (int i = 0; i < words.length; i++) {
        context.write(new Text(words[i]), ONE);
      }
    }
    // 多阶项集的情况
    else {
      SortHelper sortHelper = new SortHelperImpl();
      for (int i = 0; i < words.length; i++) {
        if ((i + n) > words.length) {
          break;
        }
        String[] itemSetArray = Arrays.copyOfRange(words, i, i + n);
        List<String> itemSetList = Arrays.asList(itemSetArray);
        itemSetList = sortHelper.sortSeq(itemSetList);  // 排序
        String itemSet = String.join(",", itemSetList);  // 以英文逗号","为分隔符进行合并
        context.write(new Text(itemSet), ONE);
      }
    }
  }
}
