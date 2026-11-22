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
package exchange.core2.core.common;

/**
 * Per-order fee overrides, per lot. A value of -1 means the symbol fee.
 * <p>
 * Resting orders and trade events keep a reference to this object instead of two long fields.
 * Orders without overrides keep null, so the hot objects stay as small as before the overrides
 * (DirectOrder 88 bytes, MatcherTradeEvent 72 bytes with compressed references).
 * Instances are immutable and can be shared between orders.
 */
public final class OrderFees {

    public final long takerFee;
    public final long makerFee;

    private OrderFees(long takerFee, long makerFee) {
        this.takerFee = takerFee;
        this.makerFee = makerFee;
    }

    /**
     * @return null when both fees are symbol defaults (-1)
     */
    public static OrderFees of(long takerFee, long makerFee) {
        return takerFee == -1 && makerFee == -1 ? null : new OrderFees(takerFee, makerFee);
    }

    public static long takerFee(OrderFees fees) {
        return fees == null ? -1 : fees.takerFee;
    }

    public static long makerFee(OrderFees fees) {
        return fees == null ? -1 : fees.makerFee;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof OrderFees)) return false;
        final OrderFees other = (OrderFees) o;
        return takerFee == other.takerFee && makerFee == other.makerFee;
    }

    @Override
    public int hashCode() {
        return 31 * Long.hashCode(takerFee) + Long.hashCode(makerFee);
    }

    @Override
    public String toString() {
        return "OrderFees{takerFee=" + takerFee + ", makerFee=" + makerFee + '}';
    }

    /**
     * Reuses instances for repeating fee pairs, so placing orders with overrides does not allocate
     * after the first order of each pair. Direct-mapped, 64 slots, allocated on first use.
     * Not thread-safe: one cache per order book.
     */
    public static final class Cache {

        private static final int SLOTS_BITS = 6;

        private OrderFees[] slots;

        public OrderFees get(long takerFee, long makerFee) {
            if (takerFee == -1 && makerFee == -1) {
                return null;
            }
            if (slots == null) {
                slots = new OrderFees[1 << SLOTS_BITS];
            }
            final int slot = (int) (((takerFee * 31 + makerFee) * 0x9E3779B97F4A7C15L) >>> (64 - SLOTS_BITS));
            OrderFees fees = slots[slot];
            if (fees == null || fees.takerFee != takerFee || fees.makerFee != makerFee) {
                fees = new OrderFees(takerFee, makerFee);
                slots[slot] = fees;
            }
            return fees;
        }
    }
}
