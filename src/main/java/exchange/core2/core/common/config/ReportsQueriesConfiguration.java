package exchange.core2.core.common.config;


import exchange.core2.core.common.api.binary.BatchAddAccountsCommand;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.binary.BinaryCommandType;
import exchange.core2.core.common.api.binary.BinaryDataCommand;
import exchange.core2.core.common.api.reports.*;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Reports configuration
 */
@Getter
public final class ReportsQueriesConfiguration {

    private final Map<Integer, Constructor<? extends ReportQuery<?>>> reportConstructors;
    private final Map<Integer, Constructor<? extends BinaryDataCommand>> binaryCommandConstructors;

    /**
     * Creates default reports config
     *
     * @return reports configuration
     */
    public static ReportsQueriesConfiguration createStandardConfig() {
        return createStandardConfig(Collections.emptyMap());
    }

    /**
     * Creates reports config with additional custom reports
     *
     * @param customReports - custom reports collection
     * @return reports configuration
     */
    public static ReportsQueriesConfiguration createStandardConfig(final Map<Integer, Class<? extends ReportQuery<?>>> customReports) {

        final Map<Integer, Constructor<? extends ReportQuery<?>>> reportConstructors = new HashMap<>();
        final Map<Integer, Constructor<? extends BinaryDataCommand>> binaryCommandConstructors = new HashMap<>();


        // binary commands (not extendable)
        addBinaryCommandClass(binaryCommandConstructors, BinaryCommandType.ADD_ACCOUNTS, BatchAddAccountsCommand.class);
        addBinaryCommandClass(binaryCommandConstructors, BinaryCommandType.ADD_SYMBOLS, BatchAddSymbolsCommand.class);

        // predefined queries (extendable)
        addQueryClass(reportConstructors, ReportType.STATE_HASH.getCode(), StateHashReportQuery.class);
        addQueryClass(reportConstructors, ReportType.SINGLE_USER_REPORT.getCode(), SingleUserReportQuery.class);
        addQueryClass(reportConstructors, ReportType.TOTAL_CURRENCY_BALANCE.getCode(), TotalCurrencyBalanceReportQuery.class);

        customReports.forEach((code, customReport) -> addQueryClass(reportConstructors, code, customReport));

        return new ReportsQueriesConfiguration(
                Collections.unmodifiableMap(reportConstructors),
                Collections.unmodifiableMap(binaryCommandConstructors));
    }


    private static void addQueryClass(final Map<Integer, Constructor<? extends ReportQuery<?>>> reportConstructors,
                                      final int reportTypeCode,
                                      final Class<? extends ReportQuery<?>> reportQueryClass) {

        final Constructor<? extends ReportQuery<?>> existing = reportConstructors.get(reportTypeCode);

        if (existing != null) {
            throw new IllegalArgumentException("Configuration error: report type code " + reportTypeCode + " is already occupied by " + existing.getDeclaringClass().getName());
        }

        try {
            reportConstructors.put(reportTypeCode, reportQueryClass.getConstructor(BytesIn.class));
        } catch (final NoSuchMethodException ex) {
            throw new IllegalArgumentException("Configuration error: report class " + reportQueryClass.getName() + "deserialization constructor accepting BytesIn");
        }

    }

    private static void addBinaryCommandClass(Map<Integer, Constructor<? extends BinaryDataCommand>> binaryCommandConstructors,
                                              BinaryCommandType type,
                                              Class<? extends BinaryDataCommand> binaryCommandClass) {
        try {
            binaryCommandConstructors.put(type.getCode(), binaryCommandClass.getConstructor(BytesIn.class));
        } catch (final NoSuchMethodException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    private ReportsQueriesConfiguration(final Map<Integer, Constructor<? extends ReportQuery<?>>> reportConstructors,
                                        final Map<Integer, Constructor<? extends BinaryDataCommand>> binaryCommandConstructors) {
        this.reportConstructors = reportConstructors;
        this.binaryCommandConstructors = binaryCommandConstructors;
    }

}
