/*
 * Copyright 2020 Maksim Zheravin
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
package exchange.core2.core.processors;

import exchange.core2.core.common.MatcherTradeEvent;
import lombok.Getter;

import java.util.concurrent.LinkedBlockingQueue;

public final class SharedPool {

    private final LinkedBlockingQueue<MatcherTradeEvent> eventChainsBuffer;

    @Getter
    private final int chainLength;

    /**
     * Create new shared pool
     *
     * @param poolMaxSize     - max size of pool. Will skip new chains if chains buffer is full.
     * @param poolInitialSize - initial number of pre-generated chains. Recommended to set higher than number of modules - (RE+ME)*2.
     * @param chainLength     - target chain length. Longer chain means rare requests for new chains. However longer chains can cause event placeholders starvation.
     */
    public SharedPool(final int poolMaxSize, final int poolInitialSize, final int chainLength) {

        if (poolInitialSize > poolMaxSize) {
            throw new IllegalArgumentException("too big poolInitialSize");
        }

        this.eventChainsBuffer = new LinkedBlockingQueue<>(poolMaxSize);
        this.chainLength = chainLength;

        for (int i = 0; i < poolInitialSize; i++) {
            final MatcherTradeEvent head = new MatcherTradeEvent();
            MatcherTradeEvent prev = head;
            for (int j = 1; j < chainLength; j++) {
                prev.nextEvent = new MatcherTradeEvent();
                prev = prev.nextEvent;
            }
            this.eventChainsBuffer.add(head);
        }
    }

    /**
     * Request next chain from buffer
     * Threadsafe
     *
     * @return chain, otherwise null
     */
    public MatcherTradeEvent getChain() {
        return eventChainsBuffer.poll();
    }

    /**
     * Offers next chain.
     * Threadsafe (single producer safety is sufficient)
     *
     * @param head - pointer to the first element
     */
    public void putChain(MatcherTradeEvent head) {
        eventChainsBuffer.offer(head);
    }

}
