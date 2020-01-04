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
package exchange.core2.tests.util;

import exchange.core2.core.common.CoreSymbolSpecification;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.BitSet;
import java.util.List;

@AllArgsConstructor
@Builder
public class TestOrdersGeneratorConfig {

    final List<CoreSymbolSpecification> coreSymbolSpecifications;
    final int totalTransactionsNumber;
    final List<BitSet> usersAccounts;
    final int targetOrderBookOrdersTotal;
    final int seed;
    final boolean hugeSizeIOC;

}
