package org.openpredict.exchange.core.journalling;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.InputStreamToWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


@Slf4j
public class
DiskSerializationProcessor implements ISerializationProcessor {

    @Override
    public void storeData(long snapshotId, SerializedModuleType type, int instanceId, WriteBytesMarshallable obj) {

        final Path path = Paths.get("./dumps/state_" + snapshotId + "_" + type + "_" + instanceId);

        log.debug("Writing state to {}", path);

        try (final OutputStream os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW);
             final OutputStream bos = new BufferedOutputStream(os)) {

            final WireToOutputStream2 wireToOutputStream = new WireToOutputStream2(WireType.RAW, bos);
            final Wire wire = wireToOutputStream.getWire();

            wire.writeBytes(obj);

            log.debug("done serializing, flushing {}", path);
            wireToOutputStream.flush();
            //bos.flush();

        } catch (final IOException ex) {
            log.error("Can not write snapshot file: ", ex);
        }

        log.debug("completed {}", path);
    }

    @Override
    public <T> T loadData(long snapshotId, SerializedModuleType type, int instanceId, Function<BytesIn, T> initFunc) {

        final Path path = Paths.get("./dumps/state_" + snapshotId + "_" + type + "_" + instanceId);

        log.debug("Loading state from {}", path);
        try (final InputStream is = Files.newInputStream(path, StandardOpenOption.READ);
             final InputStream bis = new BufferedInputStream(is)) {

            final InputStreamToWire inputStreamToWire = new InputStreamToWire(WireType.RAW, bis);
            final Wire wire = inputStreamToWire.readOne();

            log.debug("start deserializing...");

//            Bytes<?> bytes = wire.bytes();
            AtomicReference<T> ref = new AtomicReference<>();
            wire.readBytes(bytes -> ref.set(initFunc.apply(bytes)));

            return ref.get();

        } catch (final IOException ex) {
            log.error("Can not read snapshot file: ", ex);
            throw new IllegalStateException(ex);
        }
    }

    public class WireToOutputStream2 {
        private final Bytes<ByteBuffer> bytes = Bytes.elasticByteBuffer(128 * 1024 * 1024);
        private final Wire wire;
        private final DataOutputStream dos;

        public WireToOutputStream2(WireType wireType, OutputStream os) {
            wire = wireType.apply(bytes);
            dos = new DataOutputStream(os);
        }

        public Wire getWire() {
            wire.clear();
            return wire;
        }

        public void flush() throws IOException {
            int length = Math.toIntExact(bytes.readRemaining());
            dos.writeInt(length);

            final byte[] buf = new byte[1024 * 1024];

            while (bytes.readPosition() < bytes.readLimit()) {
                int read = bytes.read(buf);
                dos.write(buf, 0, read);
            }
            // TODO release buffer?
        }
    }

}
