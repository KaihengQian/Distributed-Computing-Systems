package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankReducer;
import DSPPCode.mapreduce.common_pagerank.question.PageRankRunner;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import parquet.column.page.Page;
import java.io.IOException;

public class PageRankReducerImpl extends PageRankReducer {

  // 阻尼系数
  private static final double D = 0.85;

  @Override
  public void reduce(Text key, Iterable<ReducePageRankWritable> values, Context context)
      throws IOException, InterruptedException {
    String[] pageInfo = null;
    // 从配置项中读取网页的总数
    int totalPage = context.getConfiguration().getInt(PageRankRunner.TOTAL_PAGE, 0);
    // 从配置项中读取当前迭代步数
    int iteration = context.getConfiguration().getInt(PageRankRunner.ITERATION, 0);
    double sum = 0;
    for (ReducePageRankWritable value : values) {
      String tag = value.getTag();
      // 如果是贡献值则进行求和，否则以空格为分隔符切分并保存到 pageInfo
      if (tag.equals(ReducePageRankWritable.PR_L)) {
        sum += Double.parseDouble(value.getData());
      } else if (tag.equals(ReducePageRankWritable.PAGE_INFO)) {
        pageInfo = value.getData().split(" ", 3);
      }
    }
    // 根据公式计算排名值
    double pageRank = (1 - D) / totalPage + D * sum;
    // 判断网页的排名值是否已经收敛
    double oldPageRank = Double.parseDouble(pageInfo[1]);
    if (Math.abs(pageRank - oldPageRank) < PageRankRunner.DELTA) {
      context.getCounter(PageRankRunner.GROUP_NAME, PageRankRunner.COUNTER_NAME).increment(1);
    }
    // 更新网页信息中的排名值
    pageInfo[1] = String.valueOf(pageRank);
    // 输出网页信息
    StringBuilder result = new StringBuilder();
    for (String data : pageInfo) {
      result.append(data).append(" ");
    }
    context.write(new Text(result.toString()), NullWritable.get());
  }
}
