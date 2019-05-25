package org.openpredict.exchange.core.journalling;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireToOutputStream;
import net.openhft.chronicle.wire.WireType;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.nio.file.StandardOpenOption.CREATE_NEW;

@Slf4j
public class DiskSerializationProcessor implements ISerializationProcessor {

    @Override
    public void storeData(long dumpId, SerializedModuleType type, int instanceNum, WriteBytesMarshallable obj) {

        final Path path = Paths.get("./dumps/dump_" + dumpId + "_" + type + "_" + instanceNum);

        log.debug("Writing dump to {}", path);

        try (final OutputStream os = Files.newOutputStream(path, CREATE_NEW);
             final OutputStream bos = new BufferedOutputStream(os)) {

            final WireToOutputStream wireToOutputStream = new WireToOutputStream(WireType.RAW, bos);
            final Wire wire = wireToOutputStream.getWire();

            wire.writeBytes(obj);

            log.debug("done serializing, flushing {}", path);
            wireToOutputStream.flush();
            //bos.flush();

        } catch (IOException ex) {
            log.error("Can not write dump: ", ex);
        }

        log.debug("completed {}", path);
    }
}
