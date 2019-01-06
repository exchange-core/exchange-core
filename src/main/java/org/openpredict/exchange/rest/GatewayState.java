package org.openpredict.exchange.rest;

import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;
import org.openpredict.exchange.beans.GatewaySymbolSpecification;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;


// TODO separate interfaces for admin and user
@Service
public class GatewayState {

    private final Map<String, GatewaySymbolSpecification> specsByNameIndex = new ConcurrentHashMap<>();
    private final Map<Integer, String> nameByCodeIndex = new ConcurrentHashMap<>();

    public Optional<GatewaySymbolSpecification> getSymbolSpec(String symbolName) {
        return Optional.ofNullable(specsByNameIndex.get(symbolName));
    }

    public Optional<String> getSymbolByCode(int symbol) {
        return Optional.ofNullable(nameByCodeIndex.get(symbol));
    }

    public boolean registerSymbolIfNotActive(GatewaySymbolSpecification spec) {
        String s = nameByCodeIndex.putIfAbsent(spec.symbolId, spec.symbolName);
        if (s == null || s.equals(spec.symbolName)) {
            specsByNameIndex.compute(spec.symbolName, (k, v) -> (v.active) ? v : spec);
            return true;
        } else {
            // code associated with different symbol
            return false;
        }
    }

    public GatewaySymbolSpecification activateSymbol(int symbol) {
        final String symbolName = nameByCodeIndex.get(symbol);
        if (symbolName == null) {
            return null;
        }

        GatewaySymbolSpecification inactiveSpec = specsByNameIndex.get(symbolName);
        if (inactiveSpec == null) {
            return null;
        }

        GatewaySymbolSpecification activeSpec = GatewaySymbolSpecification.builder()
                .symbolId(symbol)
                .symbolName(symbolName)
                .lotSize(inactiveSpec.lotSize)
                .priceScale(inactiveSpec.priceScale)
                .priceStep(inactiveSpec.priceStep)
                .active(true)
                .build();

        specsByNameIndex.put(symbolName, activeSpec);
        return activeSpec;
    }

}
