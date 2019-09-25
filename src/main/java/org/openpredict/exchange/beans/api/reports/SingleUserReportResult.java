package org.openpredict.exchange.beans.api.reports;


import lombok.AllArgsConstructor;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.Order;
import org.openpredict.exchange.beans.UserProfile;
import org.openpredict.exchange.core.Utils;

import java.util.List;
import java.util.stream.Stream;

@AllArgsConstructor
@Getter
public class SingleUserReportResult implements ReportResult {

    // risk engine: user profile from
    private final UserProfile userProfile;

    // matching engine: orders placed by user
    // symbol -> orders
    private final IntObjectHashMap<List<Order>> orders;

    // status
    private final ExecutionStatus status;

    private SingleUserReportResult(final BytesIn bytesIn) {
        this.userProfile = bytesIn.readBoolean() ? new UserProfile(bytesIn) : null;
        this.orders = bytesIn.readBoolean() ? Utils.readIntHashMap(bytesIn, b -> Utils.readList(b, Order::new)) : null;
        this.status = ExecutionStatus.of(bytesIn.readInt());
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {

        bytes.writeBoolean(userProfile != null);
        if (userProfile != null) {
            userProfile.writeMarshallable(bytes);
        }

        bytes.writeBoolean(orders != null);
        if (orders != null) {
            Utils.marshallIntHashMap(orders, bytes, symbolOrders -> Utils.marshallList(symbolOrders, bytes));
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

    @Override
    public String toString() {
        return "SingleUserReportResult{" +
                "userProfile=" + userProfile +
                ", orders=" + orders +
                ", status=" + status +
                '}';
    }
}
