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
package exchange.core2.tests.util;

import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.SymbolType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

import java.util.*;

@Slf4j
public final class UserCurrencyAccountsGenerator {

    /**
     * Generates random users and different currencies they should have, so the total account is between
     * accountsToCreate and accountsToCreate+currencies.size()
     * <p>
     * In average each user will have account for 4 symbols (between 1 and currencies,size)
     *
     * @param accountsToCreate
     * @param currencies
     * @return n + 1 uid records with allowed currencies
     */
    public static List<BitSet> generateUsers(final int accountsToCreate, Collection<Integer> currencies) {
        log.debug("Generating users with {} accounts ({} currencies)...", accountsToCreate, currencies.size());
        final ExecutionTime executionTime = new ExecutionTime();
        final List<BitSet> result = new ArrayList<>();
        result.add(new BitSet()); // uid=0 no accounts

        final Random rand = new Random(1);

        final RealDistribution paretoDistribution = new ParetoDistribution(new JDKRandomGenerator(0), 1, 1.5);
        final int[] currencyCodes = currencies.stream().mapToInt(i -> i).toArray();

        int totalAccountsQuota = accountsToCreate;
        do {
            // TODO prefer some currencies more
            final int accountsToOpen = Math.min(Math.min(1 + (int) paretoDistribution.sample(), currencyCodes.length), totalAccountsQuota);
            final BitSet bitSet = new BitSet();
            do {
                final int currencyCode = currencyCodes[rand.nextInt(currencyCodes.length)];
                bitSet.set(currencyCode);
            } while (bitSet.cardinality() != accountsToOpen);

            totalAccountsQuota -= accountsToOpen;
            result.add(bitSet);

//            log.debug("{}", bitSet);

        } while (totalAccountsQuota > 0);

        log.debug("Generated {} users with {} accounts up to {} different currencies in {}", result.size(), accountsToCreate, currencies.size(), executionTime.getTimeFormatted());
        return result;
    }

    public static int[] createUserListForSymbol(final List<BitSet> users2currencies, final CoreSymbolSpecification spec, int symbolMessagesExpected) {

        // we would prefer to choose from same number of users as number of messages to be generated in tests
        // at least 2 users are required, but not more than half of all users provided
        int numUsersToSelect = Math.min(users2currencies.size(), Math.max(2, symbolMessagesExpected / 5));

        final ArrayList<Integer> uids = new ArrayList<>();
        final Random rand = new Random(spec.symbolId);
        int uid = 1 + rand.nextInt(users2currencies.size() - 1);
        int c = 0;
        do {
            BitSet accounts = users2currencies.get(uid);
            if (accounts.get(spec.quoteCurrency) && (spec.type == SymbolType.FUTURES_CONTRACT || accounts.get(spec.baseCurrency))) {
                uids.add(uid);
            }
            if (++uid == users2currencies.size()) {
                uid = 1;
            }
            //uid = 1 + rand.nextInt(users2currencies.size() - 1);

            c++;
        } while (uids.size() < numUsersToSelect && c < users2currencies.size());

//        int expectedUsers = symbolMessagesExpected / 20000;
//        if (uids.size() < Math.max(2, expectedUsers)) {
//            // less than 2 uids
//            throw new IllegalStateException("Insufficient accounts density - can not find more than " + uids.size() + " matching users for symbol " + spec.symbolId
//                    + " total users:" + users2currencies.size()
//                    + " symbolMessagesExpected=" + symbolMessagesExpected
//                    + " numUsersToSelect=" + numUsersToSelect);
//        }

//        log.debug("sym: " + spec.symbolId + " " + spec.type + " uids:" + uids.size() + " msg=" + symbolMessagesExpected + " numUsersToSelect=" + numUsersToSelect + " c=" + c);

        return uids.stream().mapToInt(x -> x).toArray();
    }


}
