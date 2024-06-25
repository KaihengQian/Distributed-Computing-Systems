package DSPPCode.spark.connected_components.impl;

import DSPPCode.spark.connected_components.question.ConnectedComponents;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConnectedComponentsImpl extends ConnectedComponents {
  @Override
  public JavaPairRDD<String, Integer> getcc(JavaRDD<String> text) {
    // 将每行内容按制表符拆分，得到相邻顶点的配对
    JavaPairRDD<String, String> neighborPairs = text.flatMapToPair(line -> {
      String[] indexes = line.split("\t");
      List<Tuple2<String, String>> pairs = new ArrayList<>();
      if (indexes.length == 1) {
        pairs.add(new Tuple2<>(indexes[0], indexes[0]));
      }
      else {
        for (int i = 1; i < indexes.length; i++) {
          pairs.add(new Tuple2<>(indexes[0], indexes[i]));
        }
      }
      return pairs.iterator();
    });

    // 对每个相邻顶点的配对进行分组，得到每个顶点的所有相邻顶点
    JavaPairRDD<String, Iterable<String>> indexNeighbors = neighborPairs.groupByKey();

    // 初始化每个顶点对应的最小顶点ID为本身
    JavaPairRDD<String, Integer> min = indexNeighbors.mapToPair(tuple -> {
      String index = tuple._1;
      int minIndex = Integer.parseInt(index);
      return new Tuple2<>(index, minIndex);
    });

    // 迭代求解各连通分量的最小顶点ID
    JavaPairRDD<String, Integer> min_new = null;
    int flag = 0;
    do {
      // 首次迭代不更新
      if (flag == 0) {
        flag++;
      }
      else {
        min = min_new;
      }
      // 与所有邻居顶点对应的最小顶点ID比较，取最小者
      Map<String, Integer> minMap = min.collectAsMap();
      min_new = indexNeighbors.mapToPair(tuple -> {
        String index = tuple._1;
        int minIndex = minMap.get(index);
        Iterable<String> neighbors = tuple._2;
        for (String neighbor : neighbors) {
          if (!neighbor.equals(index)) {
            int newMinIndex = minMap.get(neighbor);
            if (newMinIndex < minIndex) {
              minIndex = newMinIndex;
            }
          }
        }
        return new Tuple2<>(index, minIndex);
      });
    } while (isChange(min, min_new));

    return min;
  }
}
