package org.openpredict.exchange.beans.api.binary;

import lombok.AllArgsConstructor;
import lombok.Getter;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.core.Utils;

@AllArgsConstructor
@Getter
public class BatchAddAccountsCommand implements WriteBytesMarshallable {

    private final LongObjectHashMap<IntLongHashMap> users;

    public BatchAddAccountsCommand(final BytesIn bytes) {
        users = Utils.readLongHashMap(bytes, c -> Utils.readIntLongHashMap(bytes));
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        Utils.marshallLongHashMap(users, Utils::marshallIntLongHashMap, bytes);
    }
}
