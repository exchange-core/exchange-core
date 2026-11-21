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

public final class ApiNop extends ApiCommand {
    @Override
    public String toString() {
        return "[NOP]";
    }

    ApiNop() {
    }

    public static ApiNopBuilder builder() {
        return new ApiNopBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiNop other
                && other.canEqual(this);
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiNop;
    }

    @Override
    public int hashCode() {
        return 1;
    }

    public static class ApiNopBuilder {
        ApiNopBuilder() {
        }

        public ApiNop build() {
            return new ApiNop();
        }

        @Override
        public String toString() {
            return "ApiNop.ApiNopBuilder()";
        }
    }
}
