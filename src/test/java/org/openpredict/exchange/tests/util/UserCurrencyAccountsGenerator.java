package org.openpredict.exchange.tests.util;

import org.apache.commons.math3.distribution.ParetoDistribution;
import org.apache.commons.math3.distribution.RealDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.SymbolType;

import java.util.*;

public class AccountsGenerator {

    /**
     * Generates random users and different currencies they should have, so the total account is between
     * accountsToCreate and accountsToCreate+currencies.size()
     *
     * @param accountsToCreate
     * @param currencies
     * @return
     */
    public static List<BitSet> generateUsers(int accountsToCreate, Collection<Integer> currencies) {

        List<BitSet> result = new ArrayList<>();
        result.add(new BitSet()); // uid=0

        final Random rand = new Random(1);

        final RealDistribution paretoDistribution = new ParetoDistribution(new JDKRandomGenerator(0), 0.75, 1.5);
        //DoubleStream.generate(paretoDistribution::sample).limit(256).forEach(System.out::println);

        int totalAccounts = 0;
        do {
            final List<Integer> curList = new ArrayList<>(currencies);
            Collections.shuffle(curList, rand);
            final int[] currs = curList.subList(0, Math.min(1 + (int) paretoDistribution.sample(), curList.size())).stream().mapToInt(a -> a).toArray();
            totalAccounts += currs.length;
            BitSet value = new BitSet();
            Arrays.stream(currs).forEach(value::set);
            result.add(value);
        } while (totalAccounts < accountsToCreate);


        return result;

    }

    /**
     * Based on weighted distribution
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

            System.out.println("sym: " + spec.symbolId + " " + spec.type + " uids:" + uids.size());

        }
        return symbolsToUsers;
    }


    public static void main(String[] args) {

        Collection<Integer> currencies = new ArrayList<>();
        for (int i = 1; i <= 32; i++) {
            currencies.add(i);
        }

        List<CoreSymbolSpecification> specs = ExchangeTestContainer.generateRandomSymbols(100000, currencies, ExchangeTestContainer.AllowedSymbolTypes.BOTH);


        long t = System.currentTimeMillis();
        List<BitSet> users2currencies = generateUsers(10000000, currencies);
        System.out.println(users2currencies.size() + " generated in " + (System.currentTimeMillis() - t) + "ms");

        //users2currencies.forEachKeyValue((k, v) -> System.out.println(k + " - " + v));


        t = System.currentTimeMillis();
        IntObjectHashMap<int[]> ints1 = assignSymbolsToUsers(users2currencies, specs, TestOrdersGenerator.createWeightedDistribution(specs.size()), 10_000_000);
        System.out.println("Mapped to users in " + (System.currentTimeMillis() - t) + "ms");


    }

}
