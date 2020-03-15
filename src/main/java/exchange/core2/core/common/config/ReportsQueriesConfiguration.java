package exchange.core2.core.common.config;


import exchange.core2.core.common.ReportType;
import exchange.core2.core.common.api.reports.ReportQuery;
import exchange.core2.core.common.api.reports.SingleUserReportQuery;
import exchange.core2.core.common.api.reports.StateHashReportQuery;
import exchange.core2.core.common.api.reports.TotalCurrencyBalanceReportQuery;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

@Getter
public final class ReportsQueriesConfiguration {

    // TODO Immutable
    private final Map<Integer, Constructor<? extends ReportQuery<?>>> reportClasses = new HashMap<>();


    public static ReportsQueriesConfiguration createStandardConfig() {

        final ReportsQueriesConfiguration cfg = new ReportsQueriesConfiguration();

        cfg.addQueryClass(ReportType.STATE_HASH.getCode(), StateHashReportQuery.class);
        cfg.addQueryClass(ReportType.SINGLE_USER_REPORT.getCode(), SingleUserReportQuery.class);
        cfg.addQueryClass(ReportType.TOTAL_CURRENCY_BALANCE.getCode(), TotalCurrencyBalanceReportQuery.class);

        return cfg;
    }


    public void addQueryClass(int reportTypeCode, Class<? extends ReportQuery<?>> reportQueryClass) {

        final Constructor<? extends ReportQuery<?>> existing = reportClasses.get(reportTypeCode);

        if (existing != null) {
            throw new IllegalArgumentException("Configuration error: report type code " + reportTypeCode + " is already occupied by " + existing.getDeclaringClass().getName());
        }

        try {
            reportClasses.put(reportTypeCode, reportQueryClass.getConstructor(BytesIn.class));
        } catch (final NoSuchMethodException ex) {
            throw new IllegalArgumentException("Configuration error: report class " + reportQueryClass.getName() + "deserialization constructor accepting BytesIn");
        }

    }

    private ReportsQueriesConfiguration() {
    }

}
