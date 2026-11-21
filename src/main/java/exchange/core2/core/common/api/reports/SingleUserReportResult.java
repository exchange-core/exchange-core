/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.common.api.reports;

import exchange.core2.core.common.Order;
import exchange.core2.core.common.PositionDirection;
import exchange.core2.core.common.UserStatus;
import exchange.core2.core.utils.SerializationUtils;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SingleUserReportResult implements ReportResult {

    public static SingleUserReportResult IDENTITY = new SingleUserReportResult(0L, null, null, null, null, QueryExecutionStatus.OK);

    private final long uid;

    // risk engine: user profile from
    //private final UserProfile userProfile;

    private final UserStatus userStatus;
    private final IntLongHashMap accounts;
    private final IntObjectHashMap<Position> positions;

    // matching engine: orders placed by user
    // symbol -> orders
    private final IntObjectHashMap<List<Order>> orders;

    // status
    private final QueryExecutionStatus queryExecutionStatus;


    public static SingleUserReportResult createFromMatchingEngine(long uid, IntObjectHashMap<List<Order>> orders) {
        return new SingleUserReportResult(uid, null, null, null, orders, QueryExecutionStatus.OK);
    }

    public static SingleUserReportResult createFromRiskEngineFound(long uid, UserStatus userStatus, IntLongHashMap accounts, IntObjectHashMap<Position> positions) {
        return new SingleUserReportResult(uid, userStatus, accounts, positions, null, QueryExecutionStatus.OK);
    }

    public static SingleUserReportResult createFromRiskEngineNotFound(long uid) {
        return new SingleUserReportResult(uid, null, null, null, null, QueryExecutionStatus.USER_NOT_FOUND);
    }

    public Map<Long, Order> fetchIndexedOrders() {
        return orders.stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(Order::getOrderId, ord -> ord));
    }

    private SingleUserReportResult(final BytesIn bytesIn) {
        this.uid = bytesIn.readLong();
//        this.userProfile = bytesIn.readBoolean() ? new UserProfile(bytesIn) : null;
        this.userStatus = bytesIn.readBoolean() ? UserStatus.of(bytesIn.readByte()) : null;
        this.accounts = bytesIn.readBoolean() ? SerializationUtils.readIntLongHashMap(bytesIn) : null;
        this.positions = bytesIn.readBoolean() ? SerializationUtils.readIntHashMap(bytesIn, Position::new) : null;
        this.orders = bytesIn.readBoolean() ? SerializationUtils.readIntHashMap(bytesIn, b -> SerializationUtils.readList(b, Order::new)) : null;
        this.queryExecutionStatus = QueryExecutionStatus.of(bytesIn.readInt());
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {

        bytes.writeLong(uid);

//        bytes.writeBoolean(userProfile != null);
//        if (userProfile != null) {
//            userProfile.writeMarshallable(bytes);
//        }

        bytes.writeBoolean(userStatus != null);
        if (userStatus != null) {
            bytes.writeByte(userStatus.getCode());
        }

        bytes.writeBoolean(accounts != null);
        if (accounts != null) {
            SerializationUtils.marshallIntLongHashMap(accounts, bytes);
        }

        bytes.writeBoolean(positions != null);
        if (positions != null) {
            SerializationUtils.marshallIntHashMap(positions, bytes);
        }

        bytes.writeBoolean(orders != null);
        if (orders != null) {
            SerializationUtils.marshallIntHashMap(orders, bytes, symbolOrders -> SerializationUtils.marshallList(symbolOrders, bytes));
        }
        bytes.writeInt(queryExecutionStatus.code);

    }

    public enum QueryExecutionStatus {
        OK(0),
        USER_NOT_FOUND(1);

        private final int code;

        QueryExecutionStatus(int code) {
            this.code = code;
        }

        public static QueryExecutionStatus of(int code) {
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
                        IDENTITY,
                        (a, b) -> new SingleUserReportResult(
                                b.uid,
//                                SerializationUtils.preferNotNull(a.userProfile, b.userProfile),
                                SerializationUtils.preferNotNull(a.userStatus, b.userStatus),
                                SerializationUtils.preferNotNull(a.accounts, b.accounts),
                                SerializationUtils.preferNotNull(a.positions, b.positions),
                                SerializationUtils.mergeOverride(a.orders, b.orders),
                                a.queryExecutionStatus != QueryExecutionStatus.OK ? a.queryExecutionStatus : b.queryExecutionStatus));
    }

    public static class Position implements WriteBytesMarshallable {

        public final int quoteCurrency;
        // open positions state (for margin trades only)
        public final PositionDirection direction;
        public final long openVolume;
        public final long openPriceSum;
        public final long profit;

        // pending orders total size
        public final long pendingSellSize;
        public final long pendingBuySize;

        private Position(BytesIn bytes) {

            this.quoteCurrency = bytes.readInt();

            this.direction = PositionDirection.of(bytes.readByte());
            this.openVolume = bytes.readLong();
            this.openPriceSum = bytes.readLong();
            this.profit = bytes.readLong();

            this.pendingSellSize = bytes.readLong();
            this.pendingBuySize = bytes.readLong();
        }

        @Override
        public void writeMarshallable(BytesOut bytes) {
            bytes.writeInt(quoteCurrency);
            bytes.writeByte((byte) direction.getMultiplier());
            bytes.writeLong(openVolume);
            bytes.writeLong(openPriceSum);
            bytes.writeLong(profit);
            bytes.writeLong(pendingSellSize);
            bytes.writeLong(pendingBuySize);
        }

        public Position(
                int quoteCurrency,
                PositionDirection direction,
                long openVolume,
                long openPriceSum,
                long profit,
                long pendingSellSize,
                long pendingBuySize) {
            this.quoteCurrency = quoteCurrency;
            this.direction = direction;
            this.openVolume = openVolume;
            this.openPriceSum = openPriceSum;
            this.profit = profit;
            this.pendingSellSize = pendingSellSize;
            this.pendingBuySize = pendingBuySize;
        }

        public int getQuoteCurrency() {
            return this.quoteCurrency;
        }

        public PositionDirection getDirection() {
            return this.direction;
        }

        public long getOpenVolume() {
            return this.openVolume;
        }

        public long getOpenPriceSum() {
            return this.openPriceSum;
        }

        public long getProfit() {
            return this.profit;
        }

        public long getPendingSellSize() {
            return this.pendingSellSize;
        }

        public long getPendingBuySize() {
            return this.pendingBuySize;
        }
    }


    @Override
    public String toString() {
        return "SingleUserReportResult{" +
                "userProfile=" + userStatus +
                ", accounts=" + accounts +
                ", orders=" + orders +
                ", queryExecutionStatus=" + queryExecutionStatus +
                '}';
    }

    private SingleUserReportResult(
            long uid,
            UserStatus userStatus,
            IntLongHashMap accounts,
            IntObjectHashMap<Position> positions,
            IntObjectHashMap<List<Order>> orders,
            QueryExecutionStatus queryExecutionStatus) {
        this.uid = uid;
        this.userStatus = userStatus;
        this.accounts = accounts;
        this.positions = positions;
        this.orders = orders;
        this.queryExecutionStatus = queryExecutionStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof SingleUserReportResult other
                && this.getUid() == other.getUid()
                && Objects.equals(this.getUserStatus(), other.getUserStatus())
                && Objects.equals(this.getAccounts(), other.getAccounts())
                && Objects.equals(this.getPositions(), other.getPositions())
                && Objects.equals(this.getOrders(), other.getOrders())
                && Objects.equals(this.getQueryExecutionStatus(), other.getQueryExecutionStatus());
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.getUid());
        var userStatus = this.getUserStatus();
        result = result * prime + (userStatus == null ? 43 : userStatus.hashCode());
        var accounts = this.getAccounts();
        result = result * prime + (accounts == null ? 43 : accounts.hashCode());
        var positions = this.getPositions();
        result = result * prime + (positions == null ? 43 : positions.hashCode());
        var orders = this.getOrders();
        result = result * prime + (orders == null ? 43 : orders.hashCode());
        var queryExecutionStatus = this.getQueryExecutionStatus();
        result = result * prime + (queryExecutionStatus == null ? 43 : queryExecutionStatus.hashCode());
        return result;
    }

    public long getUid() {
        return this.uid;
    }

    public UserStatus getUserStatus() {
        return this.userStatus;
    }

    public IntLongHashMap getAccounts() {
        return this.accounts;
    }

    public IntObjectHashMap<Position> getPositions() {
        return this.positions;
    }

    public IntObjectHashMap<List<Order>> getOrders() {
        return this.orders;
    }

    public QueryExecutionStatus getQueryExecutionStatus() {
        return this.queryExecutionStatus;
    }
}
