package DSPPCode.mapreduce.difference.impl;

import DSPPCode.mapreduce.difference.question.DifferenceMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class DifferenceMapperImpl extends DifferenceMapper {

  private final Text text = new Text();

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    // 获取输入键值对所属的 split
    FileSplit split = (FileSplit) context.getInputSplit();

    // 通过 split 获取键值对所属的文件名称
    String fileName = split.getPath().getName();

    // 以学生姓名和图书ID为键，文件名称为值
    if (fileName.contains("R")) {
      text.set("R");
      context.write(value, text);
    } else if (fileName.contains("S")) {
      text.set("S");
      context.write(value, text);
    }
  }
}
