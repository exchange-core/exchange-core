package org.openpredict.exchange.core;


import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.springframework.stereotype.Service;

@Service
public class SymbolSpecificationProvider {

    // symbol->specs
    private IntObjectHashMap<CoreSymbolSpecification> symbolSpecs = new IntObjectHashMap<>();

    public CoreSymbolSpecification getSymbolSpecification(int symbol) {
        return symbolSpecs.get(symbol);
    }

    public void registerSymbol(int symbol, CoreSymbolSpecification spec) {
        symbolSpecs.put(symbol, spec);
    }

}
