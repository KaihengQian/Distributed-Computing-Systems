package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankJoinReducer;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReduceJoinWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class PageRankJoinReducerImpl extends PageRankJoinReducer {

  @Override
  public void reduce(Text key, Iterable<ReduceJoinWritable> values, Context context)
      throws IOException, InterruptedException {
    String pageInfo = null;
    String pageRank = null;
    // 分离 values 集合中的网页连接关系和网页排名元组
    for (ReduceJoinWritable value : values) {
      // 获取 ReduceJoinWritable 对象的标识
      String tag = value.getTag();
      if (tag.equals(ReduceJoinWritable.PAGEINFO)) {
        pageInfo = value.getData();
      } else if (tag.equals(ReduceJoinWritable.PAGERNAK)) {
        pageRank = value.getData();
      }
    }

    // 进行连接操作并输出连接结果
    if (pageInfo != null && pageRank != null) {
      String[] datas = pageInfo.split(" ", 2);
      // 不重复输出网页名
      String result = pageRank + " " + datas[1];
      context.write(new Text(result), NullWritable.get());
    }
  }
}
