package DSPPCode.spark.perceptron.impl;

import DSPPCode.spark.perceptron.question.DataPoint;
import DSPPCode.spark.perceptron.question.IterationStep;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

public class IterationStepImpl extends IterationStep {
  @Override
  public Broadcast<double[]> createBroadcastVariable(JavaSparkContext sc, double[] localVariable) {
    return sc.broadcast(localVariable);
  }

  @Override
  public boolean termination(double[] old, double[] newWeightsAndBias) {
    double distance = 0.0;
    for (int i = 0; i < old.length-1; i++) {
      distance += Math.pow(newWeightsAndBias[i] - old[i], 2);
    }
    if (distance < THRESHOLD) {
      return true;
    }
    else {
      return false;
    }
  }

  @Override
  public double[] runStep(JavaRDD<DataPoint> points, Broadcast<double[]> broadcastWeightsAndBias) {
    double[] weightsAndBias = broadcastWeightsAndBias.value();
    JavaRDD<double[]> grads = points.map(new ComputeGradient(weightsAndBias));
    double[] grad = grads.reduce(new VectorSum());
    for (int i = 0; i < weightsAndBias.length; i++) {
      weightsAndBias[i] += STEP * grad[i];
    }
    return weightsAndBias;
  }

  public static class VectorSum implements Function2<double[], double[], double[]> {
    @Override
    public double[] call(double[] a, double[] b) {
      double[] result = new double[a.length];
      for (int i = 0; i < a.length; i++) {
        result[i] = a[i] + b[i];
      }
      return result;
    }
  }

  public static class ComputeGradient implements Function<DataPoint, double[]> {
    public final double[] weightsAndBias;

    public ComputeGradient(double[] weightsAndBias) {
      this.weightsAndBias = weightsAndBias;
    }

    @Override
    public double[] call(DataPoint dataPoint) {
      double[] x = dataPoint.x;
      double y = dataPoint.y;

      // 分类
      double z = 0.0;
      for (int i =0; i < x.length; i++) {
        z += x[i] * weightsAndBias[i];
      }
      z += weightsAndBias[weightsAndBias.length-1];
      int label = (z >= 0 ? 1 : -1);

      // 计算梯度
      double[] grads = new double[weightsAndBias.length];
      for (int i = 0; i < grads.length-1; i++) {
        grads[i] = (y - label) / 2 * x[i];
      }
      grads[grads.length-1] = (y - label) / 2;

      return grads;
    }
  }
}
