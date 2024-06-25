package DSPPCode.mapreduce.transitive_closure.impl;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TransitiveClosureReducerImpl extends TransitiveClosureReducer {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
    Set<String> grandChilds = new HashSet<>();  // 保存孙子女
    Set<String> grandParents = new HashSet<>();  // 保存祖父母
    String name = key.toString();

    for (Text value : values) {
      // 以一个或多个空格为分隔符进行切分
      String[] datas = value.toString().split("\\s+");
      String child = datas[0];
      String parent = datas[1];

      if (name.equals(child)) {
        grandParents.add(parent);
      }
      if (name.equals(parent)) {
        grandChilds.add(child);
      }
    }

    if ((!grandChilds.isEmpty()) && (!grandParents.isEmpty())) {
      for (String grandchild : grandChilds) {
        for (String grandparent : grandParents) {
          context.write(new Text(grandchild), new Text(grandparent));
        }
      }
    }
  }
}
