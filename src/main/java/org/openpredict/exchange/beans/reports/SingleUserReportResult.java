package org.openpredict.exchange.beans.reports;


import lombok.AllArgsConstructor;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.ReportType;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.core.Utils;

import java.util.stream.Stream;

@AllArgsConstructor
@Getter
public class SingleUserReportResult implements ReportResult {

    // risk engine: user profile from
    private final UserProfile userProfile;

    // matching engine: orders placed by user
    private final LongObjectHashMap<Order> orders;

    // status
    private final ExecutionStatus status;

    private SingleUserReportResult(final BytesIn bytesIn) {
        this.userProfile = bytesIn.readBoolean() ? new UserProfile(bytesIn) : null;
        this.orders = bytesIn.readBoolean() ? Utils.readLongHashMap(bytesIn, Order::new) : null;
        this.status = ExecutionStatus.of(bytesIn.readInt());
    }

    @Override
    public ReportType getReportType() {
        return ReportType.SINGLE_USER_REPORT;
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeBoolean(userProfile != null);
        if (userProfile != null) {
            userProfile.writeMarshallable(bytes);
        }

        bytes.writeBoolean(orders != null);
        if (orders != null) {
            Utils.marshallLongHashMap(orders, bytes);
        }
        bytes.writeInt(status.code);

    }

    public enum ExecutionStatus {
        OK(0),
        USER_NOT_FOUND(1);

        private final int code;

        ExecutionStatus(int code) {
            this.code = code;
        }

        public static ExecutionStatus of(int code) {
            switch (code) {
                case 0:
                    return OK;
                case 1:
                    return USER_NOT_FOUND;
                default:
                    throw new IllegalArgumentException("unknown ExecutionStatus:" + code);
            }
        }
    }

    public static SingleUserReportResult merge(final Stream<BytesIn> pieces) {
        return pieces
                .map(SingleUserReportResult::new)
                .reduce(
                        new SingleUserReportResult(null, null, ExecutionStatus.OK),
                        (a, b) -> new SingleUserReportResult(
                                a.userProfile == null ? b.userProfile : a.userProfile,
                                Utils.mergeOverride(a.orders, b.orders),
                                a.status != ExecutionStatus.OK ? a.status : b.status));
    }

}
