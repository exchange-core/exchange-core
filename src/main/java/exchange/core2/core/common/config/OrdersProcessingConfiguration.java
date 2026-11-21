package exchange.core2.core.common.config;

/**
 * Order processing configuration
 */
public final class OrdersProcessingConfiguration {

    public static OrdersProcessingConfiguration DEFAULT = OrdersProcessingConfiguration.builder()
            .riskProcessingMode(RiskProcessingMode.FULL_PER_CURRENCY)
            .marginTradingMode(MarginTradingMode.MARGIN_TRADING_ENABLED)
            .selfTradeProtectionMode(SelfTradeProtectionMode.IGNORE)
            .build();

    private final RiskProcessingMode riskProcessingMode;
    private final MarginTradingMode marginTradingMode;

    private final SelfTradeProtectionMode selfTradeProtectionMode;

    public OrdersProcessingConfiguration(RiskProcessingMode riskProcessingMode, MarginTradingMode marginTradingMode) {
        this(riskProcessingMode, marginTradingMode, SelfTradeProtectionMode.IGNORE);
    }

    public enum RiskProcessingMode {
        // risk processing is on, every currency/asset account is checked independently
        FULL_PER_CURRENCY,

        // risk processing is off, any orders accepted and placed
        NO_RISK_PROCESSING
    }

    public enum MarginTradingMode {
        MARGIN_TRADING_DISABLED,
        MARGIN_TRADING_ENABLED
    }

    public enum SelfTradeProtectionMode {
        // self matching allowed
        IGNORE,

        // cancel taker order when matching against own resting order
        CANCEL_NEWEST
    }

    public OrdersProcessingConfiguration(RiskProcessingMode riskProcessingMode,
                                         MarginTradingMode marginTradingMode,
                                         SelfTradeProtectionMode selfTradeProtectionMode) {
        this.riskProcessingMode = riskProcessingMode;
        this.marginTradingMode = marginTradingMode;
        this.selfTradeProtectionMode = selfTradeProtectionMode;
    }

    public static OrdersProcessingConfigurationBuilder builder() {
        return new OrdersProcessingConfigurationBuilder();
    }

    public RiskProcessingMode getRiskProcessingMode() {
        return this.riskProcessingMode;
    }

    public MarginTradingMode getMarginTradingMode() {
        return this.marginTradingMode;
    }

    public SelfTradeProtectionMode getSelfTradeProtectionMode() {
        return this.selfTradeProtectionMode;
    }

    @Override
    public String toString() {
        return "OrdersProcessingConfiguration(riskProcessingMode=" + this.getRiskProcessingMode()
                + ", marginTradingMode=" + this.getMarginTradingMode()
                + ", selfTradeProtectionMode=" + this.getSelfTradeProtectionMode() + ")";
    }

    public static class OrdersProcessingConfigurationBuilder {
        private RiskProcessingMode riskProcessingMode;
        private MarginTradingMode marginTradingMode;
        private SelfTradeProtectionMode selfTradeProtectionMode = SelfTradeProtectionMode.IGNORE;

        OrdersProcessingConfigurationBuilder() {
        }

        public OrdersProcessingConfigurationBuilder riskProcessingMode(RiskProcessingMode riskProcessingMode) {
            this.riskProcessingMode = riskProcessingMode;
            return this;
        }

        public OrdersProcessingConfigurationBuilder marginTradingMode(MarginTradingMode marginTradingMode) {
            this.marginTradingMode = marginTradingMode;
            return this;
        }

        public OrdersProcessingConfigurationBuilder selfTradeProtectionMode(SelfTradeProtectionMode selfTradeProtectionMode) {
            this.selfTradeProtectionMode = selfTradeProtectionMode;
            return this;
        }

        public OrdersProcessingConfiguration build() {
            return new OrdersProcessingConfiguration(riskProcessingMode, marginTradingMode, selfTradeProtectionMode);
        }

        @Override
        public String toString() {
            return "OrdersProcessingConfiguration.OrdersProcessingConfigurationBuilder(riskProcessingMode=" + this.riskProcessingMode
                    + ", marginTradingMode=" + this.marginTradingMode
                    + ", selfTradeProtectionMode=" + this.selfTradeProtectionMode + ")";
        }
    }
}
