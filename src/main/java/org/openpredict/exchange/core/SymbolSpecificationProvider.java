package org.openpredict.exchange.core;


import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.SymbolSpecification;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SymbolSpecificationProvider {

    // symbol->specs
    private IntObjectHashMap<SymbolSpecification> symbolSpecs = new IntObjectHashMap<>();

    public SymbolSpecification getSymbolSpecification(int symbol) {
        return symbolSpecs.get(symbol);
    }

    public void registerSymbol(int symbol, SymbolSpecification spec) {
        symbolSpecs.put(symbol, spec);
        log.debug("Registered symbol {}", symbol);
    }

}
