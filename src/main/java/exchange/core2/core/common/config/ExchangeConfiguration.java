package exchange.core2.core.common.config;

/**
 * Exchange configuration
 */
public final class ExchangeConfiguration {

    /*
     * Orders processing configuration
     */
    private final OrdersProcessingConfiguration ordersProcessingCfg;

    /*
     * Performance configuration
     */
    private final PerformanceConfiguration performanceCfg;


    /*
     * Exchange initialization configuration
     */
    private final InitialStateConfiguration initStateCfg;

    /*
     * Exchange configuration
     */
    private final ReportsQueriesConfiguration reportsQueriesCfg;

    /*
     * Logging configuration
     */
    private final LoggingConfiguration loggingCfg;

    /*
     * Serialization (snapshots and journaling) configuration
     */
    private final SerializationConfiguration serializationCfg;

    @Override
    public String toString() {
        return "ExchangeConfiguration{" +
                "\n  ordersProcessingCfg=" + ordersProcessingCfg +
                "\n  performanceCfg=" + performanceCfg +
                "\n  initStateCfg=" + initStateCfg +
                "\n  reportsQueriesCfg=" + reportsQueriesCfg +
                "\n  loggingCfg=" + loggingCfg +
                "\n  serializationCfg=" + serializationCfg +
                '}';
    }

    /**
     * Sample configuration builder having predefined default settings.
     *
     * @return configuration builder
     */
    public static ExchangeConfigurationBuilder defaultBuilder() {
        return ExchangeConfiguration.builder()
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .initStateCfg(InitialStateConfiguration.DEFAULT)
                .performanceCfg(PerformanceConfiguration.DEFAULT)
                .reportsQueriesCfg(ReportsQueriesConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DEFAULT);
    }

    public ExchangeConfiguration(
            OrdersProcessingConfiguration ordersProcessingCfg,
            PerformanceConfiguration performanceCfg,
            InitialStateConfiguration initStateCfg,
            ReportsQueriesConfiguration reportsQueriesCfg,
            LoggingConfiguration loggingCfg,
            SerializationConfiguration serializationCfg) {
        this.ordersProcessingCfg = ordersProcessingCfg;
        this.performanceCfg = performanceCfg;
        this.initStateCfg = initStateCfg;
        this.reportsQueriesCfg = reportsQueriesCfg;
        this.loggingCfg = loggingCfg;
        this.serializationCfg = serializationCfg;
    }

    public static ExchangeConfigurationBuilder builder() {
        return new ExchangeConfigurationBuilder();
    }

    public OrdersProcessingConfiguration getOrdersProcessingCfg() {
        return this.ordersProcessingCfg;
    }

    public PerformanceConfiguration getPerformanceCfg() {
        return this.performanceCfg;
    }

    public InitialStateConfiguration getInitStateCfg() {
        return this.initStateCfg;
    }

    public ReportsQueriesConfiguration getReportsQueriesCfg() {
        return this.reportsQueriesCfg;
    }

    public LoggingConfiguration getLoggingCfg() {
        return this.loggingCfg;
    }

    public SerializationConfiguration getSerializationCfg() {
        return this.serializationCfg;
    }

    public static class ExchangeConfigurationBuilder {
        private OrdersProcessingConfiguration ordersProcessingCfg;
        private PerformanceConfiguration performanceCfg;
        private InitialStateConfiguration initStateCfg;
        private ReportsQueriesConfiguration reportsQueriesCfg;
        private LoggingConfiguration loggingCfg;
        private SerializationConfiguration serializationCfg;

        ExchangeConfigurationBuilder() {
        }

        public ExchangeConfigurationBuilder ordersProcessingCfg(OrdersProcessingConfiguration ordersProcessingCfg) {
            this.ordersProcessingCfg = ordersProcessingCfg;
            return this;
        }

        public ExchangeConfigurationBuilder performanceCfg(PerformanceConfiguration performanceCfg) {
            this.performanceCfg = performanceCfg;
            return this;
        }

        public ExchangeConfigurationBuilder initStateCfg(InitialStateConfiguration initStateCfg) {
            this.initStateCfg = initStateCfg;
            return this;
        }

        public ExchangeConfigurationBuilder reportsQueriesCfg(ReportsQueriesConfiguration reportsQueriesCfg) {
            this.reportsQueriesCfg = reportsQueriesCfg;
            return this;
        }

        public ExchangeConfigurationBuilder loggingCfg(LoggingConfiguration loggingCfg) {
            this.loggingCfg = loggingCfg;
            return this;
        }

        public ExchangeConfigurationBuilder serializationCfg(SerializationConfiguration serializationCfg) {
            this.serializationCfg = serializationCfg;
            return this;
        }

        public ExchangeConfiguration build() {
            return new ExchangeConfiguration(
                    this.ordersProcessingCfg,
                    this.performanceCfg,
                    this.initStateCfg,
                    this.reportsQueriesCfg,
                    this.loggingCfg,
                    this.serializationCfg);
        }

        @Override
        public String toString() {
            return "ExchangeConfiguration.ExchangeConfigurationBuilder(ordersProcessingCfg=" + this.ordersProcessingCfg
                    + ", performanceCfg=" + this.performanceCfg
                    + ", initStateCfg=" + this.initStateCfg
                    + ", reportsQueriesCfg=" + this.reportsQueriesCfg
                    + ", loggingCfg=" + this.loggingCfg
                    + ", serializationCfg=" + this.serializationCfg + ")";
        }
    }
}
