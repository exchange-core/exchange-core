package org.openpredict.exchange.core;


import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.openpredict.exchange.beans.CoreSymbolSpecification;
import org.openpredict.exchange.beans.StateHash;
import org.openpredict.exchange.beans.cmd.CommandResultCode;

import java.util.Objects;

@Slf4j
public final class SymbolSpecificationProvider implements WriteBytesMarshallable, StateHash {

    // symbol->specs
    private final IntObjectHashMap<CoreSymbolSpecification> symbolSpecs;

    public SymbolSpecificationProvider() {
        this.symbolSpecs = new IntObjectHashMap<>();
    }

    public SymbolSpecificationProvider(BytesIn bytes) {
        this.symbolSpecs = Utils.readIntHashMap(bytes, CoreSymbolSpecification::new);
    }


    public CommandResultCode addSymbol(final CoreSymbolSpecification symbolSpecification) {
        if (getSymbolSpecification(symbolSpecification.symbolId) != null) {
            return CommandResultCode.SYMBOL_MGMT_SYMBOL_ALREADY_EXISTS;
        } else {
            registerSymbol(symbolSpecification.symbolId, symbolSpecification);
            return CommandResultCode.VALID_FOR_MATCHING_ENGINE;
        }
    }

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

    @Override
    public void writeMarshallable(BytesOut bytes) {
        // write symbolSpecs
        Utils.marshallIntHashMap(symbolSpecs, bytes);
    }

    @Override
    public int stateHash() {
        return Objects.hash(Utils.stateHash(symbolSpecs));
    }

}
