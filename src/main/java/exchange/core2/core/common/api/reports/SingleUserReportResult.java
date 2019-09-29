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
import exchange.core2.core.common.UserProfile;
import exchange.core2.core.utils.SerializationUtils;
import lombok.AllArgsConstructor;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

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
        this.orders = bytesIn.readBoolean() ? SerializationUtils.readIntHashMap(bytesIn, b -> SerializationUtils.readList(b, Order::new)) : null;
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
            SerializationUtils.marshallIntHashMap(orders, bytes, symbolOrders -> SerializationUtils.marshallList(symbolOrders, bytes));
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
                                SerializationUtils.mergeOverride(a.orders, b.orders),
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
