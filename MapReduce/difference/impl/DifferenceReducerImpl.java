package DSPPCode.mapreduce.difference.impl;

import DSPPCode.mapreduce.difference.question.DifferenceReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.I;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DifferenceReducerImpl extends DifferenceReducer {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    Iterator<Text> itr = values.iterator();
    boolean borrowed = false;
    boolean returned = false;

    while (itr.hasNext()) {
      String s = itr.next().toString();
      if (s.equals("R")) {
        borrowed = true;
      }
      if (s.equals("S")) {
        returned = true;
      }
    }

    if (borrowed && !returned) {
      context.write(key, NullWritable.get());
    }
  }
}
