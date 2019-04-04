package org.openpredict.exchange.core;


import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.CoreSymbolSpecification;

public final class SymbolSpecificationProvider {

    // symbol->specs
    private IntObjectHashMap<CoreSymbolSpecification> symbolSpecs = new IntObjectHashMap<>();

    /**
     * Get symbol specification
     *
     * @param symbol
     * @return
     */
    public CoreSymbolSpecification getSymbolSpecification(int symbol) {
        return symbolSpecs.get(symbol);
    }

    /**
     * register new symbol specification
     *
     * @param symbol
     * @param spec
     */
    public void registerSymbol(int symbol, CoreSymbolSpecification spec) {
        symbolSpecs.put(symbol, spec);
    }

    /**
     * Reset state
     */
    public void reset() {
        symbolSpecs.clear();
    }
}
