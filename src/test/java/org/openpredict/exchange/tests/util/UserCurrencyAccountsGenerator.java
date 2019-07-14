package org.openpredict.exchange.tests.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.SymbolType;

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

        List<BitSet> result = new ArrayList<>();
        result.add(new BitSet()); // uid=0 no accounts

        final Random rand = new Random(1);

        final RealDistribution paretoDistribution = new ParetoDistribution(new JDKRandomGenerator(0), 1, 1.5);

        int totalAccountsQuota = accountsToCreate;
        do {
            final List<Integer> curList = new ArrayList<>(currencies);
            // TODO prefer some currencies more
            Collections.shuffle(curList, rand);
            final int numberOfCurrencies = Math.min(Math.min(1 + (int) paretoDistribution.sample(), curList.size()), totalAccountsQuota);
            final BitSet bitSet = new BitSet();
            curList.subList(0, numberOfCurrencies).forEach(bitSet::set);
            totalAccountsQuota -= bitSet.cardinality();
            result.add(bitSet);

//            log.debug("{}", bitSet);

        } while (totalAccountsQuota > 0);

        log.debug("Generated {} users with {} accounts up to {} different currencies", result.size(), accountsToCreate, currencies.size());
        return result;
    }

    /**
     * Based on
     * - currencies of each users
     * - currencies of each symbols
     * - weighted messages2symbols distribution
     * assign users to each symbol
     *
     * @param users2currencies
     * @param symbols
     * @param distribution
     * @param totalMessagesExpected
     * @return
     */

    // symbol -> uid's having both currencies
    public static IntObjectHashMap<int[]> assignSymbolsToUsers(List<BitSet> users2currencies, List<CoreSymbolSpecification> symbols, double[] distribution, int totalMessagesExpected) {

        final double distrSum = Arrays.stream(distribution).sum();
        if (Math.abs(distrSum - 1.0) > 0.001) {
            throw new IllegalArgumentException("Distribution is not weighted, sum=" + distrSum);
        }

        IntObjectHashMap<int[]> symbolsToUsers = new IntObjectHashMap<>(symbols.size());

        final Random rand = new Random(1);

        // TODO parallel

        for (int i = 0; i < symbols.size(); i++) {
            CoreSymbolSpecification spec = symbols.get(i);

            // we would prefer to choose from same number of users as number of messages to be generated in tests
            // at least 2 users are required, but no more than half of all users provided
            int usersToSelect = Math.min(users2currencies.size() / 2, Math.max(2, (int) (distribution[i] * totalMessagesExpected)));

            ArrayList<Integer> uids = new ArrayList<>();

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
                if (c > users2currencies.size()) {
                    throw new IllegalStateException("Not sufficient number of test accounts - can not find matching users for symbol " + spec);
                }

            } while (uids.size() < usersToSelect);

            //System.out.println("sym: " + spec.symbolId + " " + spec.type + " uids:" + uids.size());

            symbolsToUsers.put(i, uids.stream().mapToInt(x -> x).toArray());
        }
        return symbolsToUsers;
    }


    public static int[] createUserListForSymbol(final List<BitSet> users2currencies, final CoreSymbolSpecification spec, int symbolMessagesExpected) {

        // we would prefer to choose from same number of users as number of messages to be generated in tests
        // at least 2 users are required, but no more than half of all users provided
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


//    public static void main(String[] args) {
//
//        Collection<Integer> currencies = new ArrayList<>();
//        for (int i = 1; i <= 32; i++) {
//            currencies.add(i);
//        }
//
//        List<CoreSymbolSpecification> specs = ExchangeTestContainer.generateRandomSymbols(100000, currencies, ExchangeTestContainer.AllowedSymbolTypes.BOTH);
//
//
//        long t = System.currentTimeMillis();
//        List<BitSet> users2currencies = generateUsers(10000000, currencies);
//        System.out.println(users2currencies.size() + " generated in " + (System.currentTimeMillis() - t) + "ms");
//
//        System.out.println("total: " + users2currencies.stream().mapToInt(BitSet::cardinality).sum());
//
//
//        //users2currencies.forEachKeyValue((k, v) -> System.out.println(k + " - " + v));
//
//
//        t = System.currentTimeMillis();
//        IntObjectHashMap<int[]> ints1 = assignSymbolsToUsers(users2currencies, specs, TestOrdersGenerator.createWeightedDistribution(specs.size()), 10_000_000);
//        System.out.println("Mapped to users in " + (System.currentTimeMillis() - t) + "ms");
//
//    }

}
