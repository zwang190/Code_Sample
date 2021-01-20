package edu.usfca.dataflow.transforms;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.CorruptedDataException;
import edu.usfca.dataflow.utils.PredictionUtils;
import edu.usfca.dataflow.utils.ProtoUtils;
import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Features {
  /**
   * This PTransform takes a PCollectionList that contains three PCollections of Strings.
   *
   * 1. DeviceProfile (output from the first pipeline) with unique DeviceIDs,
   *
   * 2. DeviceId (output from the first pipeline) that are "suspicious" (call this SuspiciousIDs), and
   *
   * 3. InAppPurchaseProfile (separately provided) with unique bundles.
   *
   * All of these proto messages are Base64-encoded (you can check ProtoUtils class for how to decode that, e.g.).
   *
   * [Step 1] First, in this PTransform, you must filter out (remove) DeviceProfiles whose DeviceIDs are found in the
   * SuspiciousIDs as we are not going to consider suspicious users.
   *
   * [Step 2] Next, you ALSO filter out (remove) DeviceProfiles whose DeviceID's UUIDs are NOT in the following form:
   *
   * ???????0-????-????-????-????????????
   *
   * Effectively, this would "sample" the dataList at rate (1/16). This sampling is mainly for efficiency reasons (later
   * when you run your pipeline on GCP, the input dataList is quite large as you will need to make "predictions" for
   * millions of DeviceIDs).
   *
   * To be clear, if " ...getUuid().charAt(7) == '0' " is true, then you process the DeviceProfile; otherwise, ignore
   * it.
   *
   * [Step 3] Then, for each user (DeviceProfile), use the method in
   * {@link edu.usfca.dataflow.utils.PredictionUtils#getInputFeatures(DeviceProfile, Map)} to obtain the user's
   * "Features" (to be used for TensorFlow model). See the comments for this method.
   *
   * Note that the said method takes in a Map (in addition to DeviceProfile) from bundles to IAPP, and thus you will
   * need to figure out how to turn PCollection into a Map. We have done this in the past (in labs & lectures).
   *
   */
  public static class GetInputToModel extends PTransform<PCollectionList<String>, PCollection<KV<DeviceId, float[]>>> {

    @Override
    public PCollection<KV<DeviceId, float[]>> expand(PCollectionList<String> pcList) {
      // TODO: If duplicate deviceIDs are found, throw CorruptedDataException.
        PCollection<DeviceId> suspicousIds =
                pcList.get(1).apply(MapElements.into(TypeDescriptor.of(DeviceId.class)).via((String b64) -> {
            try {
                return ProtoUtils.decodeMessageBase64(DeviceId.parser(), b64);
            } catch (InvalidProtocolBufferException e) {
              e.printStackTrace();
            }
            return null;
                }));

        PCollectionView<List<DeviceId>> idList = suspicousIds.apply(View.asList());

        //filtered out illegal or suspicious deviceID
        PCollection<DeviceProfile> filteredID = pcList.get(0).apply(ParDo.of(new DoFn<String, DeviceProfile>() {
            @ProcessElement
            public void process(@Element String elem, OutputReceiver<DeviceProfile> out) {
                try {
                    DeviceProfile dp = ProtoUtils.decodeMessageBase64(DeviceProfile.parser(), elem);
                    out.output(dp);
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }
        })).apply(ParDo.of(new DoFn<DeviceProfile, DeviceProfile>() {
            List<DeviceId> deviceIdList;

            @ProcessElement
            public void process(@Element DeviceProfile elem, OutputReceiver<DeviceProfile> out, ProcessContext c) {
                if (deviceIdList == null) {
                    deviceIdList = new ArrayList<>();
                    deviceIdList.addAll(c.sideInput(idList));
                }

                if (!deviceIdList.contains(elem.getDeviceId()) && elem.getDeviceId().getUuid().charAt(7) == '0') {
                    out.output(elem);
                }
            }
        }).withSideInputs(idList));

        //Filtered out duplicate ID
        filteredID.apply(ParDo.of(new DoFn<DeviceProfile, KV<String, DeviceProfile>>() {
            @ProcessElement
            public void process(@Element DeviceProfile elem, OutputReceiver<KV<String, DeviceProfile>> out) {
                out.output(KV.of(elem.getDeviceId().getUuid().toLowerCase(), elem));
            }
        })).apply(Count.perKey()).apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
            @ProcessElement
            public void process(ProcessContext c) {
                if (c.element().getValue() > 1L) {
                    throw new CorruptedDataException("Duplicate id was found in filteredID collection");
                }
            }
        }));

        PCollectionView<Map<String, Profile.InAppPurchaseProfile>> extractedMap = pcList.get(2).apply(ParDo.of(new DoFn<String, KV<String, Profile.InAppPurchaseProfile>>() {
            @ProcessElement
            public void process(@Element String elem, OutputReceiver<KV<String, Profile.InAppPurchaseProfile>> out) {
                try {
                    Profile.InAppPurchaseProfile iap = ProtoUtils.decodeMessageBase64(Profile.InAppPurchaseProfile.parser(), elem);
                    out.output(KV.of(iap.getBundle(), iap));
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                }
            }
        })).apply(View.asMap());

        return filteredID.apply(ParDo.of(new DoFn<DeviceProfile, KV<DeviceId, float[]>>() {
            Map<String, Profile.InAppPurchaseProfile> iapMap;

            @ProcessElement
            public void process(ProcessContext c) {
                if (iapMap == null) {
                    iapMap = new HashMap<>(c.sideInput(extractedMap));
                }
                c.output(KV.of(c.element().getDeviceId(), PredictionUtils.getInputFeatures(c.element(), iapMap)));
            }
        }).withSideInputs(extractedMap));
    }
  }
}
