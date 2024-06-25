package DSPPCode.spark.eulercircuit.impl;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import DSPPCode.spark.eulercircuit.question.EulerCircuit;
import scala.Tuple2;

public class EulerCircuitImpl extends EulerCircuit {

  private void dfs(JavaPairRDD<String, Iterable<String>> schoolNeighbors, String school, Set<String> visitedSchools) {
    if (!visitedSchools.contains(school)) {
      visitedSchools.add(school);
      Iterable<String> neighbors = schoolNeighbors.lookup(school).get(0);
      for (String neighbor : neighbors) {
        dfs(schoolNeighbors, neighbor, visitedSchools);
      }
    }
  }

  @Override
  public boolean isEulerCircuit(JavaRDD<String> lines, JavaSparkContext jsc) {
    // 将每行内容按空格拆分，得到相邻学院的配对
    JavaPairRDD<String, String> neighborPairs = lines.flatMapToPair(line -> {
      String[] schools = line.split(" ");
      List<Tuple2<String, String>> pairs = new ArrayList<>();
      pairs.add(new Tuple2<>(schools[0], schools[1]));
      pairs.add(new Tuple2<>(schools[1], schools[0]));
      return pairs.iterator();
    });

    // 对每个相邻学院的配对进行分组，得到每个学院的所有相邻学院
    JavaPairRDD<String, Iterable<String>> schoolNeighbors = neighborPairs.groupByKey();

    // 检查图的连通性
    List<String> schools = schoolNeighbors.keys().collect();
    String startSchool = schools.get(0);
    Set<String> visitedSchools = new HashSet<>();
    dfs(schoolNeighbors, startSchool, visitedSchools);
    if (!visitedSchools.containsAll(schools)) {
      return false;
    }

    // 计算每个顶点（学院）的度数
    JavaPairRDD<String, Integer> schoolDegrees = schoolNeighbors.mapToPair(tuple -> {
      String school = tuple._1;
      Iterable<String> neighbors = tuple._2;
      int degree = 0;
      for (String neighbor : neighbors) {
        degree++;
      }
      return new Tuple2<>(school, degree);
    });

    // 检查度数为奇数的顶点个数
    long oddDegreeCount = schoolDegrees
        .filter(tuple -> tuple._2 % 2 != 0)
        .count();

    // 如果没有度数为奇数的顶点，则存在闭合欧拉回路
    return oddDegreeCount == 0;
  }
}
