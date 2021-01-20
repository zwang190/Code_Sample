package edu.usfca.dataflow.transforms;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Profile.AppProfile;
import edu.usfca.protobuf.Profile.DeviceProfile;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SuspiciousIDs {
  private static final Logger LOG = LoggerFactory.getLogger(SuspiciousIDs.class);

  /**
   * This method serves to flag certain users as suspicious.
   *
   * (1) USER_COUNT_THRESHOLD: This determines whether an app is popular or not.
   *
   * Any app that has more than "USER_COUNT_THRESHOLD" unique users is considered popular.
   *
   * Default value is 4 (so, 5 or more users = popular).
   *
   *
   * (2) APP_COUNT_THRESHOLD: If a user (DeviceProfile) contains more than "APP_COUNT_THRESHOLD" unpopular apps,
   *
   * then the user is considered suspicious.
   *
   * Default value is 3 (so, 4 or more unpopular apps = suspicious).
   *
   *
   * (3) GEO_COUNT_THRESHOLD: If a user (DeviceProfile) contains more than "GEO_COUNT_THRESHOLD" unique Geo's,
   *
   * then the user is considered suspicious.
   *
   * Default value is 8 (so, 9 or more distinct Geo's = suspicious).
   *
   * 
   * (4) BID_LOG_COUNT_THRESHOLD: If a user (DeviceProfile) appeared in more than "BID_LOG_COUNT_THRESHOLD" Bid Logs,
   *
   * then the user is considered suspicious (we're not counting invalid BidLogs for this part as it should have been
   * ignored from the beginning).
   *
   * Default value is 10 (so, 11 or more valid BidLogs from the same user = suspicious).
   *
   * 
   * NOTE: When you run your pipelines on GCP, we'll not use the default values for these thresholds (see the document).
   *
   * The default values are mainly for unit tests (so you can easily check correctness with rather small threshold
   * values).
   */

  public static PCollection<DeviceId> getSuspiciousIDs(//
      PCollection<DeviceProfile> dps, //
      PCollection<AppProfile> aps, //
      int USER_COUNT_THRESHOLD, // Default is 4 for unit tests.
      int APP_COUNT_THRESHOLD, // Default is 3 for unit tests.
      int GEO_COUNT_THRESHOLD, // Default is 8 for unit tests.
      int BID_LOG_COUNT_THRESHOLD // Default is 10 for unit tests.
  ) {
    LOG.info("[Thresholds] user count {} app count {} geo count {} bid log count {}", USER_COUNT_THRESHOLD,
        APP_COUNT_THRESHOLD, GEO_COUNT_THRESHOLD, BID_LOG_COUNT_THRESHOLD);

    PCollectionView<List<String>> newAppProfileList = aps.apply(ParDo.of(new DoFn<AppProfile, String>() {
      @ProcessElement
      public void process(@Element AppProfile elem, OutputReceiver<String> out) {
        if (elem.getUserCount() > USER_COUNT_THRESHOLD) {
          out.output(elem.getBundle());
        }
      }
    })).apply(View.asList());

    PCollection<DeviceId> res = dps.apply(ParDo.of(new DoFn<DeviceProfile, DeviceId>() {
      List<String> dpList;
      @ProcessElement
      public void process(ProcessContext c) {
        boolean appCountFlag = false;
        boolean geoCountFlag = false;
        boolean bidCountFlag = false;
        DeviceProfile dp = c.element();
        //Set<AppProfile> dpList = new HashSet<>(c.sideInput(newAppProfileList));
        if (dpList == null) {
          dpList = new ArrayList<>();
          dpList.addAll(c.sideInput(newAppProfileList));
        }

//        Set<String> bundleSet = new HashSet<>();
//        for (AppProfile app : dpList) {
//          bundleSet.add(app.getBundle().toLowerCase());
//        }

        int count = 0;
        int bidSum = 0;
        for (DeviceProfile.AppActivity appa : dp.getAppList()) {
          if (!dpList.contains(appa.getBundle().toLowerCase())) {
            count++;
          }

          Map<Integer, Integer> exchangeMap = appa.getCountPerExchangeMap();
          for (Integer x : exchangeMap.keySet()) {
            bidSum = bidSum + exchangeMap.get(x);
          }
        }

        if (count > APP_COUNT_THRESHOLD) {
          appCountFlag = true;
        }

        if (dp.getGeoList().size() > GEO_COUNT_THRESHOLD) {
          geoCountFlag = true;
        }

        if (bidSum > BID_LOG_COUNT_THRESHOLD) {
          bidCountFlag = true;
        }

        if (appCountFlag || geoCountFlag || bidCountFlag) {
          c.output(c.element().getDeviceId());
        }
      }
    }).withSideInputs(newAppProfileList));
    return res;
  }
}
