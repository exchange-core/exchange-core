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
package exchange.core2.core.common.api;

import exchange.core2.core.common.api.binary.BinaryDataCommand;

import java.util.Objects;

public final class ApiBinaryDataCommand extends ApiCommand {

    // transfer unique id
    // can be constant unless going to push data concurrently
    public final int transferId;

    // serializable object
    public final BinaryDataCommand data;

    @Override
    public String toString() {
        return "[BINARY_DATA tid=" + transferId + " data=" + data + "]";
    }

    public ApiBinaryDataCommand(int transferId, BinaryDataCommand data) {
        this.transferId = transferId;
        this.data = data;
    }

    public static ApiBinaryDataCommandBuilder builder() {
        return new ApiBinaryDataCommandBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiBinaryDataCommand other
                && other.canEqual(this)
                && this.transferId == other.transferId
                && Objects.equals(this.data, other.data);
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiBinaryDataCommand;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + this.transferId;
        var data = this.data;
        result = result * prime + (data == null ? 43 : data.hashCode());
        return result;
    }

    public static class ApiBinaryDataCommandBuilder {
        private int transferId;
        private BinaryDataCommand data;

        ApiBinaryDataCommandBuilder() {
        }

        public ApiBinaryDataCommandBuilder transferId(int transferId) {
            this.transferId = transferId;
            return this;
        }

        public ApiBinaryDataCommandBuilder data(BinaryDataCommand data) {
            this.data = data;
            return this;
        }

        public ApiBinaryDataCommand build() {
            return new ApiBinaryDataCommand(this.transferId, this.data);
        }

        @Override
        public String toString() {
            return "ApiBinaryDataCommand.ApiBinaryDataCommandBuilder(transferId=" + this.transferId
                    + ", data=" + this.data + ")";
        }
    }
}
