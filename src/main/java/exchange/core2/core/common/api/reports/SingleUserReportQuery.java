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
import exchange.core2.core.processors.MatchingEngineRouter;
import exchange.core2.core.processors.RiskEngine;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

@EqualsAndHashCode
@ToString
@Slf4j
public final class SingleUserReportQuery implements ReportQuery<SingleUserReportResult> {

    private final long uid;

    public SingleUserReportQuery(long uid) {
        this.uid = uid;
    }

    public SingleUserReportQuery(final BytesIn bytesIn) {
        this.uid = bytesIn.readLong();
    }

    public long getUid() {
        return uid;
    }

    @Override
    public int getReportTypeCode() {
        return ReportType.SINGLE_USER_REPORT.getCode();
    }

    @Override
    public SingleUserReportResult createResult(final Stream<BytesIn> sections) {
        return SingleUserReportResult.merge(sections);
    }

    @Override
    public Optional<SingleUserReportResult> process(final MatchingEngineRouter matchingEngine) {
        final IntObjectHashMap<List<Order>> orders = new IntObjectHashMap<>();

        matchingEngine.getOrderBooks().forEach(ob -> {
            final List<Order> userOrders = ob.findUserOrders(this.uid);
            // dont put empty results, so that the report result merge procedure would be simple
            if (!userOrders.isEmpty()) {
                orders.put(ob.getSymbolSpec().symbolId, userOrders);
            }
        });

        //log.debug("ME{}: orders: {}", matchingEngine.getShardId(), orders);
        return Optional.of(SingleUserReportResult.createFromMatchingEngine(uid, orders));
    }

    @Override
    public Optional<SingleUserReportResult> process(final RiskEngine riskEngine) {

        if (!riskEngine.uidForThisHandler(this.uid)) {
            return Optional.empty();
        }
        final UserProfile userProfile = riskEngine.getUserProfileService().getUserProfile(this.uid);

        if (userProfile != null) {
            final IntObjectHashMap<SingleUserReportResult.Position> positions = new IntObjectHashMap<>(userProfile.positions.size());
            userProfile.positions.forEachKeyValue((symbol, pos) ->
                    positions.put(symbol, new SingleUserReportResult.Position(
                            pos.currency,
                            pos.direction,
                            pos.openVolume,
                            pos.openPriceSum,
                            pos.profit,
                            pos.pendingSellSize,
                            pos.pendingBuySize)));

            return Optional.of(SingleUserReportResult.createFromRiskEngineFound(
                    uid,
                    userProfile.userStatus,
                    userProfile.accounts,
                    positions));
        } else {
            // not found
            return Optional.of(SingleUserReportResult.createFromRiskEngineNotFound(uid));
        }
    }

    @Override
    public void writeMarshallable(final BytesOut bytes) {
        bytes.writeLong(uid);
    }
}
