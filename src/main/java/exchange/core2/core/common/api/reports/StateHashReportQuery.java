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

import exchange.core2.core.processors.MatchingEngineRouter;
import exchange.core2.core.processors.RiskEngine;
import exchange.core2.core.utils.HashingUtils;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;

import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Stream;

@NoArgsConstructor
@EqualsAndHashCode
@ToString
@Slf4j
public final class StateHashReportQuery implements ReportQuery<StateHashReportResult> {

    public StateHashReportQuery(BytesIn bytesIn) {
        // do nothing
    }

    @Override
    public int getReportTypeCode() {
        return ReportType.STATE_HASH.getCode();
    }

    @Override
    public StateHashReportResult createResult(Stream<BytesIn> sections) {
        return StateHashReportResult.merge(sections);
    }

    @Override
    public Optional<StateHashReportResult> process(MatchingEngineRouter matchingEngine) {

        final SortedMap<StateHashReportResult.SubmoduleKey, Integer> hashCodes = new TreeMap<>();

        final int moduleId = matchingEngine.getShardId();

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.MATCHING_BINARY_CMD_PROCESSOR),
                matchingEngine.getBinaryCommandsProcessor().stateHash());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.MATCHING_ORDER_BOOKS),
                HashingUtils.stateHash(matchingEngine.getOrderBooks()));

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.MATCHING_SHARD_MASK),
                Long.hashCode(matchingEngine.getShardMask()));

        return Optional.of(
                new StateHashReportResult(hashCodes));
    }

    @Override
    public Optional<StateHashReportResult> process(RiskEngine riskEngine) {

        final SortedMap<StateHashReportResult.SubmoduleKey, Integer> hashCodes = new TreeMap<>();

        final int moduleId = riskEngine.getShardId();

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_SYMBOL_SPEC_PROVIDER),
                riskEngine.getBinaryCommandsProcessor().stateHash());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_USER_PROFILE_SERVICE),
                riskEngine.getUserProfileService().stateHash());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_BINARY_CMD_PROCESSOR),
                riskEngine.getBinaryCommandsProcessor().stateHash());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_LAST_PRICE_CACHE),
                HashingUtils.stateHash(riskEngine.getLastPriceCache()));

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_FEES),
                riskEngine.getFees().hashCode());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_ADJUSTMENTS),
                riskEngine.getAdjustments().hashCode());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_SUSPENDS),
                riskEngine.getSuspends().hashCode());

        hashCodes.put(
                StateHashReportResult.createKey(moduleId, StateHashReportResult.SubmoduleType.RISK_SHARD_MASK),
                Long.hashCode(riskEngine.getShardMask()));

        return Optional.of(
                new StateHashReportResult(hashCodes));
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        // do nothing
    }
}
