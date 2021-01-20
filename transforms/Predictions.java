package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Data.PredictionData;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Tensor;

import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Predictions {

  /**
   * This method will be called by the unit tests.
   *
   * The reason for having this method (instead of instantiating a specific DoFn) is to allow you to easily experiment
   * with different implementations of PredictDoFn.
   *
   * The provided code (see below) for "PredictDoFnNever" is "correct" but extremely inefficient.
   *
   * Use it as a reference to implement "PredictDoFn" instead.
   *
   * When you are ready to optimize it, you'll find the ungraded homework for Lab 09 useful (as well as sample code from
   * L34: DF-TF).
   */
  public static DoFn<KV<DeviceId, float[]>, PredictionData> getPredictDoFn(String pathToModelDir) {
    return new PredictDoFn(pathToModelDir);
  }

  // This utility method simply returns the index with largest prediction score.
  // Input must be an array of length 10.
  // This is provided for you (see PredictDoFnNever" to understand how it's used).
  static int getArgMax(float[] pred) {
    int prediction = -1;
    for (int j = 0; j < 10; j++) {
      if (prediction == -1 || pred[prediction] < pred[j]) {
        prediction = j;
      }
    }
    return prediction;
  }

  /**
   * TODO: Use this starter code to implement your own PredictDoFn.
   *
   * You'll need to utilize DoFn's annotated methods & optimization techniques that we discussed in L10, L30, L34, and
   * Lab09.
   */
  static class PredictDoFn extends DoFn<KV<DeviceId, float[]>, PredictionData> {
    final String pathToModelDir;
    final static String tfTag = "serve";

    transient Tensor rate;
    transient SavedModelBundle mlBundle;
    List<KV<DeviceId, float[]>> buffer;

    final int BUFFER_MAX_SIZE = 200;
    float[][] batchPrediction;

    public PredictDoFn(String pathToModelDir) {
      this.pathToModelDir = pathToModelDir;
    }

    float[][] getPrediction(float[][] inputFeatures, SavedModelBundle mlBundle) {
      try (Tensor<?> x = Tensor.create(inputFeatures)) {
        try (Tensor<?> output = mlBundle.session().runner().feed("input_tensor", x).feed("dropout/keep_prob", rate)
                .fetch("output_tensor").run().get(0)) {
          output.copyTo(batchPrediction);
        }
      }
      return batchPrediction;
    }

    @Setup
    public void setup() {
      mlBundle = SavedModelBundle.load(pathToModelDir, tfTag);
      float[] keep_prob_arr = new float[1024];
      Arrays.fill(keep_prob_arr, 1f);
      rate = Tensor.create(new long[]{1, 1024}, FloatBuffer.wrap(keep_prob_arr));
    }

    @StartBundle
    public void startBundle() {
      buffer = new ArrayList<>();
      batchPrediction = new float[BUFFER_MAX_SIZE][10];
    }

    @ProcessElement
    public void process(ProcessContext c) {
      buffer.add(c.element());
      if (buffer.size() > BUFFER_MAX_SIZE) {
        flush(c);
      }
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext c) {
      batchPrediction = new float[buffer.size()][10];
      float[][] inputData = new float[buffer.size()][];
      for (int i = 0; i < buffer.size(); i++) {
        inputData[i] = buffer.get(i).getValue();
      }

      float[][] pred = getPrediction(inputData, mlBundle);

      for (int i = 0; i < buffer.size(); i++) {
        int prediction = getArgMax(pred[i]);
        c.output(PredictionData.newBuilder().setId(buffer.get(i).getKey()).setPrediction(prediction).setScore(pred[i][prediction]).build(), Instant.EPOCH, GlobalWindow.INSTANCE);
      }
      buffer.clear();

      // Note that "FinishBundleContext c" can be used to "output" elements here.
      // Example is shown below. The second and third args are meaningless for us (since we're doing batch processing).
      // c.output(null, Instant.EPOCH, GlobalWindow.INSTANCE);
    }

    public void flush(ProcessContext c) {
      float[][] inputData = new float[BUFFER_MAX_SIZE][];
      for (int i = 0; i < BUFFER_MAX_SIZE; i++) {
        inputData[i] = buffer.get(i).getValue();
      }

      float[][] pred = getPrediction(inputData, mlBundle);

      for (int i = 0; i < BUFFER_MAX_SIZE; i++) {
        int prediction = getArgMax(pred[i]);
        c.output(PredictionData.newBuilder().setId(buffer.get(i).getKey()).setScore(pred[i][prediction]).build());
      }
    }
  }
}
