/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.processors.journalling;

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
public final class DiskSerializationProcessor implements ISerializationProcessor {

    private final Path dumpsFolder;

    public DiskSerializationProcessor(final Path dumpsFolder) {
        this.dumpsFolder = dumpsFolder;
    }

    public DiskSerializationProcessor(final String dumpsFolder) {
        this.dumpsFolder = Paths.get(dumpsFolder);
    }

    @Override
    public boolean storeData(long snapshotId, SerializedModuleType type, int instanceId, WriteBytesMarshallable obj) {

        final Path path = resolvePath(snapshotId, type, instanceId);

        log.debug("Writing state to {} ...", path);

        try (final OutputStream os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW);
             final OutputStream bos = new BufferedOutputStream(os);
             final WireToOutputStream2 wireToOutputStream = new WireToOutputStream2(WireType.RAW, bos)) {

            final Wire wire = wireToOutputStream.getWire();

            wire.writeBytes(obj);

            log.debug("done serializing, flushing {} ...", path);
            wireToOutputStream.flush();
            //bos.flush();
            log.debug("completed {}", path);
            return true;

        } catch (final IOException ex) {
            log.error("Can not write snapshot file: ", ex);
            return false;
        }
    }

    private Path resolvePath(long snapshotId, SerializedModuleType type, int instanceId) {
        return dumpsFolder.resolve("state_" + snapshotId + "_" + type + "_" + instanceId);
    }

    @Override
    public <T> T loadData(long snapshotId, SerializedModuleType type, int instanceId, Function<BytesIn, T> initFunc) {

        final Path path = resolvePath(snapshotId, type, instanceId);

        log.debug("Loading state from {}", path);
        try (final InputStream is = Files.newInputStream(path, StandardOpenOption.READ);
             final InputStream bis = new BufferedInputStream(is)) {

            // TODO improve reading algorithm
            final InputStreamToWire inputStreamToWire = new InputStreamToWire(WireType.RAW, bis);
            final Wire wire = inputStreamToWire.readOne();

            log.debug("start de-serializing...");

            AtomicReference<T> ref = new AtomicReference<>();
            wire.readBytes(bytes -> ref.set(initFunc.apply(bytes)));

            return ref.get();

        } catch (final IOException ex) {
            log.error("Can not read snapshot file: ", ex);
            throw new IllegalStateException(ex);
        }
    }

    public class WireToOutputStream2 implements AutoCloseable {
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
        }

        @Override
        public void close() {
            bytes.release();
        }
    }

}
