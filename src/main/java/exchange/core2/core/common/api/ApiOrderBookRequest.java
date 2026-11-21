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

public final class ApiOrderBookRequest extends ApiCommand {

    final public int symbol;

    final public int size;

    @Override
    public String toString() {
        return "[OB " + symbol + " " + size + "]";
    }

    public ApiOrderBookRequest(int symbol, int size) {
        this.symbol = symbol;
        this.size = size;
    }

    public static ApiOrderBookRequestBuilder builder() {
        return new ApiOrderBookRequestBuilder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof ApiOrderBookRequest other
                && other.canEqual(this)
                && this.symbol == other.symbol
                && this.size == other.size;
    }

    protected boolean canEqual(Object other) {
        return other instanceof ApiOrderBookRequest;
    }

    @Override
    public int hashCode() {
        final int prime = 59;
        int result = 1;
        result = result * prime + this.symbol;
        result = result * prime + this.size;
        return result;
    }

    public static class ApiOrderBookRequestBuilder {
        private int symbol;
        private int size;

        ApiOrderBookRequestBuilder() {
        }

        public ApiOrderBookRequestBuilder symbol(int symbol) {
            this.symbol = symbol;
            return this;
        }

        public ApiOrderBookRequestBuilder size(int size) {
            this.size = size;
            return this;
        }

        public ApiOrderBookRequest build() {
            return new ApiOrderBookRequest(this.symbol, this.size);
        }

        @Override
        public String toString() {
            return "ApiOrderBookRequest.ApiOrderBookRequestBuilder(symbol=" + this.symbol + ", size=" + this.size + ")";
        }
    }
}
