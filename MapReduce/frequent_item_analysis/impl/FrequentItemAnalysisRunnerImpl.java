package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.FrequentItemAnalysisRunner;
import DSPPCode.mapreduce.frequent_item_analysis.question.SortHelper;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;
import java.net.URISyntaxException;

public class FrequentItemAnalysisRunnerImpl extends FrequentItemAnalysisRunner {
  @Override
  public void configureMapReduceTask(Job job)
      throws IOException, URISyntaxException {
    job.setCombinerClass(FrequentItemAnalysisCombinerImpl.class);
  }
}
