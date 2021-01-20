package edu.usfca.dataflow.transforms;

import edu.usfca.dataflow.CorruptedDataException;
import edu.usfca.dataflow.utils.DeviceProfileUtils;
import edu.usfca.protobuf.Common;
import edu.usfca.protobuf.Profile.AppProfile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import edu.usfca.protobuf.opt.BidOpt.ExchangeOpt;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.Iterator;

public class AppProfiles {
  /**
   * "ComputeAppProfiles" takes in one PCollection of DeviceProfiles.
   * <p>
   * If the input PCollection contains any duplicate Device IDs (recall that uuid is case-insensitive),
   * <p>
   * then it must throw "CorruptedDataException".
   * <p>
   * Otherwise, proceed to produce one AppProfile proto per "bundle" (String, case-sensitive).
   * <p>
   * For each bundle (app), you should aggregate:
   * <p>
   * (1) "bundle": This is unique key (String) for each AppProfile, and is case-sensitive.
   * <p>
   * (2) "user_count": This is the unique number of users (Device IDs) who have this app in their DeviceProfile's
   * AppActivity.
   * <p>
   * (3) "user_count_per_exchange": Same as (2), but it's a map from "Exchange" enum (its integer value) to the number
   * of unique DeviceIDs.
   * <p>
   * (Note that this is simplified when compared to Project 2.)
   * <p>
   * TODO: You can use instructor's reference code from project 2 and modify it (you'll need to fix a couple of things),
   * or reuse yours. Note that either way you'll have to make changes because the requirements / proto definitions have
   * changed slightly (things are simplified).
   */
  public static class ComputeAppProfiles extends PTransform<PCollection<DeviceProfile>, PCollection<AppProfile>> {

    @Override
    public PCollection<AppProfile> expand(PCollection<DeviceProfile> input) {

      // check condition (a). (This is given to you in order to show you how you should handle cases like this.)
      if (input == null) {
        throw new CorruptedDataException("PCollectionList is null or its size is not 2");
      }

      checkId(getIdAndDpForAppProfiles(input));

      final int offset = 1;
      final int arrSize = ExchangeOpt.CS_VALUE + offset + 1;
      return input.apply(ParDo.of(new EmitData())).apply(Count.perElement())
              .apply(ParDo.of(new DoFn<KV<KV<String, Integer>, Long>, KV<String, KV<Integer, Long>>>() {
                @ProcessElement
                public void process(ProcessContext c) {
                  c.output(
                          KV.of(c.element().getKey().getKey(), KV.of(c.element().getKey().getValue(), c.element().getValue())));
                }
              })).apply(Combine.perKey(new Combine.CombineFn<KV<Integer, Long>, int[], AppProfile>() {
                @Override
                public int[] createAccumulator() {
                  return new int[arrSize];
                }

                @Override
                public int[] addInput(int[] mutableAccumulator, KV<Integer, Long> input) {
                  mutableAccumulator[input.getKey() + offset] =
                          mutableAccumulator[input.getKey() + offset] + input.getValue().intValue();
                  return mutableAccumulator;
                }

                @Override
                public int[] mergeAccumulators(Iterable<int[]> accumulators) {
                  Iterator<int[]> it = accumulators.iterator();
                  int[] first = null;
                  while (it.hasNext()) {
                    int[] next = it.next();
                    if (first == null)
                      first = next;
                    else {
                      for (int i = 0; i < arrSize; i++) {
                        first[i] += next[i];
                      }
                    }
                  }

                  return first;
                }

                @Override
                public AppProfile extractOutput(int[] accumulator) {
                  AppProfile.Builder ap = AppProfile.newBuilder();
                  ap.setUserCount(accumulator[LIFE_COUNT + offset]);
                  for (ExchangeOpt exchange : ExchangeOpt.values()) {
                    if (exchange == ExchangeOpt.UNRECOGNIZED) {
                      continue;
                    }

                    if (accumulator[exchange.getNumber() + offset] != 0) {
                      ap.putUserCountPerExchange(exchange.getNumber(), accumulator[exchange.getNumber() + offset]);
                    }
                  }
                  return ap.build();
                }
              })).apply(MapElements.into(TypeDescriptor.of(AppProfile.class))
                      .via((KV<String, AppProfile> x) -> x.getValue().toBuilder().setBundle(x.getKey()).build()));
    }

    public static PCollection<KV<Common.DeviceId, DeviceProfile>> getIdAndDpForAppProfiles(PCollection<DeviceProfile> collection) {
      PCollection<KV<Common.DeviceId, DeviceProfile>> res;
      res = collection.apply("GenerateDPWithKV", ParDo.of(new DoFn<DeviceProfile, KV<Common.DeviceId, DeviceProfile>>() {
        @ProcessElement
        public void process(@Element DeviceProfile elem, OutputReceiver<KV<Common.DeviceId, DeviceProfile>> out) {
          if (!DeviceProfileUtils.isDpValid(elem)) {
            throw new CorruptedDataException("DeviceId is invalid");
          }
          Common.DeviceId canonical = elem.getDeviceId().toBuilder().setUuid(elem.getDeviceId().getUuid().toLowerCase()).build();
          out.output(KV.of(canonical, elem.toBuilder().setDeviceId(canonical).build()));
        }
      }));

      return res;
    }

    public static void checkId(PCollection<KV<Common.DeviceId, DeviceProfile>> input) {
      input.apply(Count.perKey())
              .apply("CheckDuplicate", ParDo.of(new DoFn<KV<Common.DeviceId, Long>, String>() {
                @ProcessElement
                public void process(@Element KV<Common.DeviceId, Long> elem) {
                  if (elem.getValue() != 1)
                    throw new CorruptedDataException("Duplicate deviceId is detected");
                }
              }));
    }
  }

  static final int LIFE_COUNT = -1;

  static class EmitData extends DoFn<DeviceProfile, KV<String, Integer>> {

    @ProcessElement
    public void process(ProcessContext c) {
      DeviceProfile dp = c.element();
      for (DeviceProfile.AppActivity app : dp.getAppList()) {
        c.output(KV.of(app.getBundle(), LIFE_COUNT));
        for (int exchange : app.getCountPerExchangeMap().keySet()) {
          if (exchange < 0) {
            continue;
          }
          c.output(KV.of(app.getBundle(), exchange));
        }
      }
    }
  }
}
