package org.openpredict.exchange.rest;

import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;
import org.openpredict.exchange.beans.GatewaySymbolSpecification;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;


// TODO separate interfaces for admin and user
@Service
public class GatewayState {

    private final Map<String, GatewaySymbolSpecification> specsMap = new ConcurrentHashMap<>();

    public Optional<GatewaySymbolSpecification> getSymbolId(String symbolName) {
        return Optional.ofNullable(specsMap.get(symbolName));
    }

    public void registerSymbolId(GatewaySymbolSpecification spec) {
        specsMap.putIfAbsent(spec.symbolName, spec);
    }

}
