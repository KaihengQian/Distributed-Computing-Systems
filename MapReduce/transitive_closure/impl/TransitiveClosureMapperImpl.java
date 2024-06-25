package DSPPCode.mapreduce.transitive_closure.impl;

import DSPPCode.mapreduce.transitive_closure.question.TransitiveClosureMapper;
import org.apache.hadoop.io.Text;
import org.stringtemplate.v4.ST;
import java.io.IOException;

public class TransitiveClosureMapperImpl extends TransitiveClosureMapper {
  
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    // 以一个或多个空格为分隔符进行切分
    String[] datas = value.toString().split("\\s+");
    String child = datas[0];
    String parent = datas[1];
    // 跳过第一行关系标识
    if ((!child.equals("child")) && (!parent.equals("parent"))) {
      context.write(new Text(child), value);
      context.write(new Text(parent), value);
    }
  }
}
