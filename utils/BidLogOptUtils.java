package edu.usfca.dataflow.utils;

import edu.usfca.protobuf.Common.DeviceId;
import edu.usfca.protobuf.Common.OsType;
import edu.usfca.protobuf.Profile.DeviceProfile;
import edu.usfca.protobuf.Profile.DeviceProfile.AppActivity;
import edu.usfca.protobuf.Profile.DeviceProfile.GeoActivity;
import edu.usfca.protobuf.opt.BidOpt.BidLogOpt;
import edu.usfca.protobuf.opt.BidOpt.BidResultOpt;
import edu.usfca.protobuf.opt.BidOpt.ExchangeOpt;
import org.apache.commons.lang3.StringUtils;

import java.util.UUID;

/**
 * This file was copied from reference solution of a previous project, and slightly modified for the purpose of this
 * project.
 * <p>
 * You can assume that all methods in this class are correct (once you fix a silly bug in some methods).
 * <p>
 * It is up to you whether you re-use this code or you write your own (which may be more efficient).
 * <p>
 * Also, if you are unsure about something or have questions, feel free to ask on Piazza (but also do check Project 2
 * instructions as you may find what you need faster that way).
 * <p>
 * (It's strongly recommended that you use every single class/method provided here, because that will simplify a lot of
 * things in your code.)
 */

public class BidLogOptUtils {
    /**
     * (You can assume that this method is correct.)
     * <p>
     * * A BidLog proto message is considered invalid if any of the following is true:
     * <p>
     * (1) exchange is unknown (has enum value of 0)
     * <p>
     * (2) bid_result is unknown (has enum value of 0)
     * <p>
     * (3) bid_price <= 0 when bid_result is "BID" (see comments in bid.proto)
     * <p>
     * (4) bid_price != 0 when bid_result is not "BID" (see comments in bid.proto)
     * <p>
     * (5) received_at <= 0 (see comments in bid.proto)
     * <p>
     * (6) processed_at <= received_at (see comments in bid.proto)
     * <p>
     * (7) Not applicable.
     * <p>
     * (8) bid_request.device.os is neither "ios" nor "android" (case-insensitive)
     * <p>
     * (9) bid_request.device.ifa is an invalid uuid (use {@link java.util.UUID#fromString} to check)
     * <p>
     * (10) bid_request.app.bundle is blank (use {@link org.apache.commons.lang3.StringUtils#isBlank})
     * <p>
     * (11) bid_request.device.geo.country is blank (use {@link org.apache.commons.lang3.StringUtils#isBlank})
     * <p>
     * (12) bid_request.device.geo.region is blank (use {@link org.apache.commons.lang3.StringUtils#isBlank})
     * <p>
     * If none of the above is true, then return true (aka the given bidlog is valid).
     */
    public static boolean isValid(BidLogOpt bidLog) {
        if (bidLog.getExchange() == ExchangeOpt.UNKNOWN_EXCHANGE) {
            return false;
        }
        if (bidLog.getBidResult() == BidResultOpt.UNKNOWN_RESULT) {
            return false;
        }
        if (bidLog.getBidPrice() <= 0 && bidLog.getBidResult() == BidResultOpt.BID) {
            return false;
        }
        if (bidLog.getBidPrice() != 0 && bidLog.getBidResult() != BidResultOpt.BID) {
            return false;
        }
        if (bidLog.getReceivedAt() <= 0) {
            return false;
        }
        if (bidLog.getProcessedAt() <= bidLog.getReceivedAt()) {
            return false;
        }
        if (getOsType(bidLog.getBidRequestOpt().getDevice().getOs()) == OsType.UNKNOWN_OS_TYPE) {
            return false;
        }
        try {
            UUID.fromString(bidLog.getBidRequestOpt().getDevice().getIfa());
        } catch (IllegalArgumentException e) {
            return false;
        }
        if (StringUtils.isBlank(bidLog.getBidRequestOpt().getApp().getBundle())) {
            return false;
        }
        if (StringUtils.isBlank(bidLog.getBidRequestOpt().getDevice().getGeo().getCountry())) {
            return false;
        }
        if (StringUtils.isBlank(bidLog.getBidRequestOpt().getDevice().getGeo().getRegion())) {
            return false;
        }
        return true;
    }

    /**
     * Helper method for {@link #getDeviceProfile(BidLogOpt)}.
     * <p>
     * TODO: Fix a small bug in this!
     */
    private static OsType getOsType(String os) {
        if ("android".equalsIgnoreCase(os)) {
            return OsType.ANDROID;
        } else if ("ios".equalsIgnoreCase(os)) {
            return OsType.IOS;
        } else {
            return OsType.UNKNOWN_OS_TYPE;
        }
    }

    /**
     * This returns a correct DeviceProfile proto, given BidLog proto.
     * <p>
     * TODO: Fix a small bug in this!
     *
     * @throws IllegalArgumentException if the input element is not valid (use {@link #isValid(BidLogOpt)}.
     */
    public static DeviceProfile getDeviceProfile(BidLogOpt elem) {
        if (!isValid(elem)) {
            throw new IllegalArgumentException("invalid bidlog");
        }

        final String os = elem.getBidRequestOpt().getDevice().getOs();
        final String uuid = elem.getBidRequestOpt().getDevice().getIfa();
        final long receivedAt = elem.getReceivedAt();
        final String bundle = elem.getBidRequestOpt().getApp().getBundle();
        final ExchangeOpt exchange = elem.getExchange();
        final String country = elem.getBidRequestOpt().getDevice().getGeo().getCountry();
        final String region = elem.getBidRequestOpt().getDevice().getGeo().getRegion();

        final DeviceId deviceId = DeviceId.newBuilder().setOs(getOsType(os)).setUuid(uuid.toUpperCase()).build();
        final GeoActivity geo = GeoActivity.newBuilder().setCountry(country).setRegion(region).build();

        DeviceProfile.Builder dp = DeviceProfile.newBuilder().setDeviceId(deviceId).setFirstAt(receivedAt)
                .setLastAt(receivedAt).addGeo(geo).addApp(AppActivity.newBuilder().setBundle(bundle).setFirstAt(receivedAt)
                        .setLastAt(receivedAt).putCountPerExchange(exchange.getNumber(), 1).build());

        return dp.build();
    }
}
