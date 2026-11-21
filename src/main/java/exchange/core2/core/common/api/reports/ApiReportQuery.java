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

import java.util.Objects;

public final class ApiReportQuery {

    public long timestamp;

    // transfer unique id
    // can be constant unless going to push data concurrently
    public final int transferId;

    // serializable object
    public final ReportQuery<?> query;

    @Override
    public String toString() {
        return "[REPORT_QUERY tid=" + transferId + " query=" + query + "]";
    }

    ApiReportQuery(long timestamp, int transferId, ReportQuery<?> query) {
        this.timestamp = timestamp;
        this.transferId = transferId;
        this.query = query;
    }

    public static ApiReportQueryBuilder builder() {
        return new ApiReportQueryBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiReportQuery other
                && this.timestamp == other.timestamp
                && this.transferId == other.transferId
                && Objects.equals(this.query, other.query);
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.timestamp);
        result = result * prime + this.transferId;
        var query = this.query;
        result = result * prime + (query == null ? 43 : query.hashCode());
        return result;
    }

    public static class ApiReportQueryBuilder {
        private long timestamp;
        private int transferId;
        private ReportQuery<?> query;

        ApiReportQueryBuilder() {
        }

        public ApiReportQueryBuilder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public ApiReportQueryBuilder transferId(int transferId) {
            this.transferId = transferId;
            return this;
        }

        public ApiReportQueryBuilder query(ReportQuery<?> query) {
            this.query = query;
            return this;
        }

        public ApiReportQuery build() {
            return new ApiReportQuery(this.timestamp, this.transferId, this.query);
        }

        @Override
        public String toString() {
            return "ApiReportQuery.ApiReportQueryBuilder(timestamp=" + this.timestamp
                    + ", transferId=" + this.transferId
                    + ", query=" + this.query + ")";
        }
    }
}
