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
package exchange.core2.core.orderbook;

import lombok.AllArgsConstructor;

import java.util.Spliterator;
import java.util.function.Consumer;


@AllArgsConstructor
public class OrdersSpliterator implements Spliterator<OrderBookDirectImpl.DirectOrder> {

    private OrderBookDirectImpl.DirectOrder pointer;

    @Override
    public boolean tryAdvance(Consumer<? super OrderBookDirectImpl.DirectOrder> action) {
        if (pointer == null) {
            return false;
        } else {
            action.accept(pointer);
            pointer = pointer.prev;
            return true;
        }
    }

    @Override
    public Spliterator<OrderBookDirectImpl.DirectOrder> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return Spliterator.ORDERED;
    }
}
