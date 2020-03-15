package exchange.core2.core.common.api.binary;

import net.openhft.chronicle.bytes.WriteBytesMarshallable;

public interface BinaryDataCommand extends WriteBytesMarshallable {

    int getBinaryCommandTypeCode();

}
