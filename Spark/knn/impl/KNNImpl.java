package DSPPCode.spark.knn.impl;

import DSPPCode.spark.knn.question.Data;
import DSPPCode.spark.knn.question.KNN;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.reflect.ClassTag$;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public class KNNImpl extends KNN {

  public KNNImpl(int k) {
    super(k);
  }

  @Override
  public JavaPairRDD<Data, Data> kNNJoin(JavaRDD<Data> trainData, JavaRDD<Data> queryData) {
    // 广播查询数据
    Broadcast<List<Data>> broadcastQueryData = trainData.context().broadcast(queryData.collect(), ClassTag$.MODULE$.apply(List.class));
    return trainData.flatMapToPair(train -> {
      List<Tuple2<Data, Data>> results = new ArrayList<>();
      List<Data> queries = broadcastQueryData.value();
      for (Data query : queries) {
        results.add(new Tuple2<>(query, train));
      }
      return results.iterator();
    });
  }

  @Override
  public JavaPairRDD<Integer, Tuple2<Integer, Double>> calculateDistance(JavaPairRDD<Data, Data> data) {
    return data.mapToPair(tuple -> {
      int queryId = tuple._1.id;  // 查询数据id
      double[] queryX = tuple._1.x;  // 查询数据特征
      int trainY = tuple._2.y;  // 训练数据类别id
      double[] trainX = tuple._2.x;  // 训练数据特征

      double distance = 0.0;
      for (int i = 0; i < queryX.length; i++) {
        distance += Math.pow(queryX[i] - trainX[i], 2);
      }

      return new Tuple2<>(queryId, new Tuple2<>(trainY, Math.sqrt(distance)));
    });
  }

  @Override
  public JavaPairRDD<Integer, Integer> classify(JavaPairRDD<Integer, Tuple2<Integer, Double>> data) {
    JavaPairRDD<Integer, Iterable<Tuple2<Integer, Double>>> queryDistance = data.groupByKey();
    return queryDistance.mapToPair(tuple -> {
      int queryId = tuple._1;
      Iterable<Tuple2<Integer, Double>> distances = tuple._2;

      // 使用优先队列维护前k个最近邻
      PriorityQueue<Tuple2<Integer, Double>> pq = new PriorityQueue<>(k, (a, b) -> Double.compare(b._2, a._2));
      for (Tuple2<Integer, Double> distance : distances) {
        pq.offer(distance);
        if (pq.size() > k) {
          pq.poll();
        }
      }

      // 统计前k个数据类别id
      Map<Integer, Integer> count = new HashMap<>();
      while (!pq.isEmpty()) {
        Tuple2<Integer, Double> neighbor = pq.poll();
        count.compute(neighbor._1, (key, value) -> value == null ? 1 : value + 1);
      }
      // 寻找权重最大的类别
      int queryY = -1;
      int times = -1;
      for (Map.Entry<Integer, Integer> entry : count.entrySet()) {
        int y = entry.getKey();
        int newTimes = entry.getValue();
        if (newTimes > times) {
          queryY = y;
          times = newTimes;
        } else if (newTimes == times && y < queryY) {
          queryY = y;
        }
      }

      return new Tuple2<>(queryId, queryY);
    });
  }
}
