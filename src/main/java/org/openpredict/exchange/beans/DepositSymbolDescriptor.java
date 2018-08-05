package org.openpredict.exchange.beans;

public class DepositSymbolDescriptor {

    public int symbolCode;
    public long depositBuy;
    public long depositSell;

    public long protfolioAmount = 0; // can be negative or positive
    public long ordersBuy = 0; // can be 0 or positive
    public long ordersSell = 0; // can be 0 or positive

    public DepositSymbolDescriptor(int symbolCode, long depositBuy, long depositSell) {
        this.symbolCode = symbolCode;
        this.depositBuy = depositBuy;
        this.depositSell = depositSell;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GSD{");
        sb.append(" CODE: ").append(symbolCode);
        sb.append(" P:").append(protfolioAmount);
        sb.append(" O2B:").append(ordersBuy);
        sb.append(" O2S:").append(ordersSell);
        sb.append(" requiredDepositBuy:").append(depositBuy);
        sb.append(" requiredDepositSell:").append(depositSell);
        sb.append('}');
        return sb.toString();
    }
}
