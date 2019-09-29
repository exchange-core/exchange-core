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
package exchange.core2.core.processors;

import com.lmax.disruptor.ExceptionHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;

@Slf4j
@RequiredArgsConstructor
public final class DisruptorExceptionHandler<T> implements ExceptionHandler<T> {

    public final String name;
    public final BiConsumer<Throwable, Long> onException;

    @Override
    public void handleEventException(Throwable ex, long sequence, T event) {
        log.debug("Disruptor '{}' seq={} caught exception: {}", name, sequence, event, ex);
        onException.accept(ex, sequence);
    }

    @Override
    public void handleOnStartException(Throwable ex) {
        log.debug("Disruptor '{}' startup exception: {}", name, ex);
    }

    @Override
    public void handleOnShutdownException(Throwable ex) {
        log.debug("Disruptor '{}' shutdown exception: {}", name, ex);
    }
}
