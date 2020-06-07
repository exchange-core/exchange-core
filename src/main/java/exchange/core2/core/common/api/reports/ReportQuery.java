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
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Reports query interface.
 *
 * @param <T> corresponding result type
 */
public interface ReportQuery<T extends ReportResult> extends WriteBytesMarshallable {

    /**
     * @return report type code (integer)
     */
    int getReportTypeCode();

    /**
     * @return report map-reduce constructor
     */
    T createResult(Stream<BytesIn> sections);

    /**
     * Report main logic.
     * This method is executed by matcher engine thread.
     *
     * @param matchingEngine matcher engine instance
     * @return custom result
     */
    Optional<T> process(MatchingEngineRouter matchingEngine);

    /**
     * Report main logic
     * This method is executed by risk engine thread.
     *
     * @param riskEngine risk engine instance.
     * @return custom result
     */
    Optional<T> process(RiskEngine riskEngine);
}
