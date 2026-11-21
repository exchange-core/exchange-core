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

public final class ApiPersistState extends ApiCommand {

    public long dumpId;
    public boolean seal;

    @Override
    public String toString() {
        return "[PERSIST]-" + dumpId + " seal=" + seal;
    }

    ApiPersistState(long dumpId, boolean seal) {
        this.dumpId = dumpId;
        this.seal = seal;
    }

    public static ApiPersistStateBuilder builder() {
        return new ApiPersistStateBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiPersistState other
                && other.canEqual(this)
                && this.dumpId == other.dumpId
                && this.seal == other.seal;
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiPersistState;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + Long.hashCode(this.dumpId);
        result = result * prime + (this.seal ? 79 : 97);
        return result;
    }

    public static class ApiPersistStateBuilder {
        private long dumpId;
        private boolean seal;

        ApiPersistStateBuilder() {
        }

        public ApiPersistStateBuilder dumpId(long dumpId) {
            this.dumpId = dumpId;
            return this;
        }

        public ApiPersistStateBuilder seal(boolean seal) {
            this.seal = seal;
            return this;
        }

        public ApiPersistState build() {
            return new ApiPersistState(this.dumpId, this.seal);
        }

        @Override
        public String toString() {
            return "ApiPersistState.ApiPersistStateBuilder(dumpId=" + this.dumpId + ", seal=" + this.seal + ")";
        }
    }
}
