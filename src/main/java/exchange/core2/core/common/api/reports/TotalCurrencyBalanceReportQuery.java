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

import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.PositionDirection;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.processors.MatchingEngineRouter;
import exchange.core2.core.processors.RiskEngine;
import exchange.core2.core.processors.SymbolSpecificationProvider;
import exchange.core2.core.utils.CoreArithmeticUtils;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.Optional;
import java.util.stream.Stream;

@NoArgsConstructor
@EqualsAndHashCode
@ToString
public final class TotalCurrencyBalanceReportQuery implements ReportQuery<TotalCurrencyBalanceReportResult> {

    public TotalCurrencyBalanceReportQuery(BytesIn bytesIn) {
        // do nothing
    }

    @Override
    public int getReportTypeCode() {
        return ReportType.TOTAL_CURRENCY_BALANCE.getCode();
    }

    @Override
    public TotalCurrencyBalanceReportResult createResult(final Stream<BytesIn> sections) {
        return TotalCurrencyBalanceReportResult.merge(sections);
    }

    @Override
    public Optional<TotalCurrencyBalanceReportResult> process(final MatchingEngineRouter matchingEngine) {

        final IntLongHashMap currencyBalance = new IntLongHashMap();

        matchingEngine.getOrderBooks().stream()
                .filter(ob -> ob.getSymbolSpec().type == SymbolType.CURRENCY_EXCHANGE_PAIR)
                .forEach(ob -> {
                    final CoreSymbolSpecification spec = ob.getSymbolSpec();

                    currencyBalance.addToValue(
                            spec.getBaseCurrency(),
                            ob.askOrdersStream(false).mapToLong(ord -> CoreArithmeticUtils.calculateAmountAsk(ord.getSize() - ord.getFilled(), spec)).sum());

                    currencyBalance.addToValue(
                            spec.getQuoteCurrency(),
                            ob.bidOrdersStream(false).mapToLong(ord -> CoreArithmeticUtils.calculateAmountBidTakerFee(ord.getSize() - ord.getFilled(), ord.getReserveBidPrice(), spec)).sum());
                });

        return Optional.of(TotalCurrencyBalanceReportResult.ofOrderBalances(currencyBalance));
    }

    @Override
    public Optional<TotalCurrencyBalanceReportResult> process(final RiskEngine riskEngine) {

        // prepare fast price cache for profit estimation with some price (exact value is not important, except ask==bid condition)
        final IntObjectHashMap<RiskEngine.LastPriceCacheRecord> dummyLastPriceCache = new IntObjectHashMap<>();
        riskEngine.getLastPriceCache().forEachKeyValue((s, r) -> dummyLastPriceCache.put(s, r.averagingRecord()));

        final IntLongHashMap currencyBalance = new IntLongHashMap();

        final IntLongHashMap symbolOpenInterestLong = new IntLongHashMap();
        final IntLongHashMap symbolOpenInterestShort = new IntLongHashMap();

        final SymbolSpecificationProvider symbolSpecificationProvider = riskEngine.getSymbolSpecificationProvider();

        riskEngine.getUserProfileService().getUserProfiles().forEach(userProfile -> {
            userProfile.accounts.forEachKeyValue(currencyBalance::addToValue);
            userProfile.positions.forEachKeyValue((symbolId, positionRecord) -> {
                final CoreSymbolSpecification spec = symbolSpecificationProvider.getSymbolSpecification(symbolId);
                final RiskEngine.LastPriceCacheRecord avgPrice = dummyLastPriceCache.getIfAbsentPut(symbolId, RiskEngine.LastPriceCacheRecord.dummy);
                currencyBalance.addToValue(positionRecord.currency, positionRecord.estimateProfit(spec, avgPrice));

                if (positionRecord.direction == PositionDirection.LONG) {
                    symbolOpenInterestLong.addToValue(symbolId, positionRecord.openVolume);
                } else if (positionRecord.direction == PositionDirection.SHORT) {
                    symbolOpenInterestShort.addToValue(symbolId, positionRecord.openVolume);
                }
            });
        });

        return Optional.of(
                new TotalCurrencyBalanceReportResult(
                        currencyBalance,
                        new IntLongHashMap(riskEngine.getFees()),
                        new IntLongHashMap(riskEngine.getAdjustments()),
                        new IntLongHashMap(riskEngine.getSuspends()),
                        null,
                        symbolOpenInterestLong,
                        symbolOpenInterestShort));
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        // do nothing
    }
}
