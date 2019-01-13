package org.openpredict.exchange.rest;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.impl.map.mutable.ConcurrentHashMap;
import org.openpredict.exchange.beans.GatewaySymbolSpecification;
import org.rapidoid.http.Req;
import org.rapidoid.http.Resp;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;


// TODO separate interfaces for admin and user
@Service
@Slf4j
public class GatewayState {


    public final AtomicInteger syncRequestsSequence = new AtomicInteger(0);
    public final Map<Integer, Resp> syncRequests = new ConcurrentHashMap<>();

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
            specsByNameIndex.compute(spec.symbolName, (k, v) -> (v != null && v.active) ? v : spec);
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


    public Resp doAsyncCall(Req req, IntConsumer asyncMethod) {
        final int ticket = syncRequestsSequence.incrementAndGet();
        Resp resp = req.async().response();
        asyncMethod.accept(ticket);
        syncRequests.put(ticket, resp);
        return resp;
    }
}
