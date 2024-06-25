package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankJoinMapper;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReduceJoinWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class PageRankJoinMapperImpl extends PageRankJoinMapper {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 获取输入键值对所属的 split
    FileSplit split = (FileSplit) context.getInputSplit();
    // 通过 split 获取键值对所属的文件路径
    String path = split.getPath().toString();
    ReduceJoinWritable writable = new ReduceJoinWritable();
    // 使用 writable 的 data 保存元组
    writable.setData(value.toString());
    // 以空格为分隔符切分元组，以便获取网页名
    String[] datas = value.toString().split(" ");
    // 通过输入数据所属文件路径判断 datas 的表来源并进行分类处理
    if (path.contains("pages")) {
      // 标识 data 保存的元组来自网页连接关系
      writable.setTag(ReduceJoinWritable.PAGEINFO);
      // 以网页名为键输出结果
      context.write(new Text(datas[0]), writable);
    } else if (path.contains("ranks")) {
      // 标识 data 保存的元组来自网页排名
      writable.setTag(ReduceJoinWritable.PAGERNAK);
      // 以网页名为键输出结果
      context.write(new Text(datas[0]), writable);
    }
  }
}
