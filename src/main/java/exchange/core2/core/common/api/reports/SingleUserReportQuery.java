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
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

@EqualsAndHashCode
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
    public Function<Stream<BytesIn>, SingleUserReportResult> getResultBuilder() {
        return SingleUserReportResult::merge;
    }

    @Override
    public Optional<SingleUserReportResult> process(MatchingEngineRouter matchingEngine) {
        final IntObjectHashMap<List<Order>> orders = new IntObjectHashMap<>();
        matchingEngine.getOrderBooks().forEach(ob -> orders.put(ob.getSymbolSpec().symbolId, ob.findUserOrders(this.uid)));

        //log.debug("orders: {}", orders.size());
        return Optional.of(new SingleUserReportResult(uid, null, null, null, orders, SingleUserReportResult.QueryExecutionStatus.OK));
    }

    @Override
    public Optional<SingleUserReportResult> process(RiskEngine riskEngine) {

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
