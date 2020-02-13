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
package exchange.core2.core.processors.journaling;

import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import lombok.RequiredArgsConstructor;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


@Slf4j
@RequiredArgsConstructor
public final class DiskSerializationProcessor implements ISerializationProcessor {


    private static final long MB = 1024 * 1024;
    private static final long FILE_SIZE_TRIGGER = 4 * 1024 * MB; // split files by size
    private static final String DATE_FORMAT = "yyyy-MM-dd_HHmmss";

    private static final int BUFFER_SIZE = 128 * 1024;
    private static final int BUFFER_FLUSH_TRIGGER = BUFFER_SIZE - 256;


    private final String exchangeId; // TODO validate
    private final Path folder;


    private long baseSnapshotId;

    private RandomAccessFile raf;
    private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private int filesCounter = 0;

    private long writtenBytes = 0;

    private final String today = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DATE_FORMAT));


    public DiskSerializationProcessor(final String exchangeId, final Path folder, final long baseSnapshotId) {
        this.exchangeId = exchangeId;
        this.folder = folder;
        this.baseSnapshotId = baseSnapshotId;
    }

    public DiskSerializationProcessor(final String exchangeId, final String folder, final long baseSnapshotId) {
        this.exchangeId = exchangeId;
        this.folder = Paths.get(folder);
        this.baseSnapshotId = baseSnapshotId;
    }


    @Override
    public boolean storeData(long snapshotId, long seq, SerializedModuleType type, int instanceId, WriteBytesMarshallable obj) {

        final Path path = resolveSnapshotPath(snapshotId, type, instanceId);

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

        } catch (final IOException ex) {
            log.error("Can not write snapshot file: ", ex);
            return false;
        }

        synchronized (this) {
            try (final OutputStream os = Files.newOutputStream(resolveMainLogPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                os.write((System.currentTimeMillis() + " seq=" + seq + " snapshotId=" + snapshotId + " type=" + type.code + " instance=" + instanceId + "\n").getBytes());
            } catch (final IOException ex) {
                log.error("Can not write main log file: ", ex);
                return false;
            }
        }

        return true;

    }

    @Override
    public <T> T loadData(long snapshotId, SerializedModuleType type, int instanceId, Function<BytesIn, T> initFunc) {

        final Path path = resolveSnapshotPath(snapshotId, type, instanceId);

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


    // TODO asynchronously create new file and then switch reference

    @Override
    public void writeToJournal(OrderCommand cmd, long seq, boolean eob) throws IOException {

//        log.debug("Writing {}", cmd);

        //buffer.putInt(cmd.symbol); // TODO Header

        // TODO allocate big buffer, just move pointer

        final OrderCommandType cmdType = cmd.command;

        if (!cmdType.isMutate()) {
            // skip commands
            return;
        }

        if (raf == null) {
            startNewFile();
        }


        // 9 bytes mandatory fields
        buffer.putLong(cmd.timestamp); // 8 bytes - can be compressed as delta
        buffer.put(cmdType.getCode()); // 1 byte

        if (cmdType == OrderCommandType.MOVE_ORDER) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary
            buffer.putLong(cmd.symbol); // 4 bytes can be compressed as dictionary
            buffer.putLong(cmd.orderId); // 8 bytes - can be compressed as delta
            buffer.putLong(cmd.price); // 8 bytes - can be compressed as delta

        } else if (cmdType == OrderCommandType.CANCEL_ORDER) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary
            buffer.putLong(cmd.symbol); // 4 bytes can be compressed as dictionary
            buffer.putLong(cmd.orderId); // 8 bytes - can be compressed as delta

        } else if (cmdType == OrderCommandType.PLACE_ORDER) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary
            buffer.putLong(cmd.symbol); // 4 bytes can be compressed as dictionary
            buffer.putLong(cmd.orderId); // 8 bytes - can be compressed as delta
            buffer.putLong(cmd.price); // 8 bytes - can be compressed as delta
            buffer.putLong(cmd.reserveBidPrice); // 8 bytes - can be compressed (diff to price or 0)
            buffer.putLong(cmd.size); // 8 bytes - can be compressed

            final int actionAndType = (cmd.action.getCode() << 2) & cmd.orderType.getCode();
            buffer.put((byte) actionAndType); // 1 byte

        } else if (cmdType == OrderCommandType.BALANCE_ADJUSTMENT) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary
            buffer.putLong(cmd.symbol); // 4 bytes can be compressed as dictionary (currency)
            buffer.putLong(cmd.orderId); // 8 bytes can be compressed as delta (transaction)
            buffer.putLong(cmd.price); // 8 bytes - can be compressed as low value (amount)
            buffer.put(cmd.orderType.getCode()); // 1 byte (adjustment or suspend)

        } else if (cmdType == OrderCommandType.ADD_USER ||
                cmdType == OrderCommandType.SUSPEND_USER ||
                cmdType == OrderCommandType.RESUME_USER) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as delta

        } else if (cmdType == OrderCommandType.BINARY_DATA_COMMAND) {

            buffer.putLong((byte) cmd.symbol); // 1 byte (0 or -1)
            buffer.putLong(cmd.orderId); // 8 bytes word0
            buffer.putLong(cmd.price); // 8 bytes word1
            buffer.putLong(cmd.reserveBidPrice); // 8 bytes word2
            buffer.putLong(cmd.size); // 8 bytes word3
            buffer.putLong(cmd.uid); // 8 bytes word4

        } else if (cmdType == OrderCommandType.PERSIST_STATE_MATCHING ||
                cmdType == OrderCommandType.PERSIST_STATE_RISK) {
            buffer.putLong(cmd.orderId); // 8 bytes
        }

        if (cmdType == OrderCommandType.PERSIST_STATE_RISK) {
            // start new file
            baseSnapshotId = cmd.orderId;
            flushBufferSync(true);

        } else if (eob || buffer.position() >= BUFFER_FLUSH_TRIGGER) {
            // flushing on end of batch or when buffer is full
            // log.debug("Flushing {} bytes", buffer.position());
            flushBufferSync(false);
        }

    }

    private void flushBufferSync(final boolean forceNewFile) throws IOException {
        raf.write(buffer.array(), 0, buffer.position());
        writtenBytes += buffer.position();
        buffer.clear();

        if (forceNewFile || writtenBytes >= FILE_SIZE_TRIGGER) {
            // todo start preparing new file asynchronously, but ONLY ONCE
            startNewFile();
            writtenBytes = 0;
        }
    }

    //@PostConstruct
    private void startNewFile() throws IOException {
        filesCounter++;
        if (raf != null) {
            raf.close();
        }
        raf = new RandomAccessFile(resolveJournalPath(filesCounter).toString(), "rwd");
    }
//
//    public void replayJournal(long lastTimestamp){
//
//        try {
//            File file = new File(filename);
//            session.folder = file.getParentFile();
//            log.info("Reading descriptor: {} Folder: {}", session.fileName, session.folder);
//
//            try (FileInputStream fileInputStream = new FileInputStream(file)) {
//
//                session.descriptorFileInputStream = fileInputStream;
//                session.descriptorInputStream = new BufferedInputStream(fileInputStream);
//
//                while (session.descriptorInputStream.available() > 0) {
//                    SystemDataRecordType recordType = readSystemDataRecordType(session);
//
//                    if (session.debug > 0) log.debug("........ Parsing {} .......", recordType);
//
//                    if (recordType.equals(SystemDataRecordType.STATUS)) {
//                        readStatus(session);
//                    } else if (recordType.equals(SystemDataRecordType.ADD_SYMBOL)) {
////                        readAddSymbol(session, executorService);
//                    } else if (recordType.equals(SystemDataRecordType.VERSION_INFO)) {
////                        readVersionInfo(session);
//                    } else {
//                        throw new DataReadException("Unsupported record type:" + recordType);
//                    }
//                }
//
//            } catch (DataReadException e) {
//                e.addMoreDeatails(null);
//                throw e;
//            }
//
//            // TODO index .fdr files - can load using fork-join-pool to avoid waiting for biggest file to finish
//            // wait for all threads and merge events
//            for (Future<List<MarketEnvelope>> symbolFeature : session.symbolFeatures) {
//                // TODO fix incorrect order
//                session.list.addAll(symbolFeature.get());
//            }
//        } catch (InterruptedException e) {
//
//
//    }
//


    private Path resolveSnapshotPath(long snapshotId, SerializedModuleType type, int instanceId) {

        return folder.resolve(String.format("%s_snapshot_%d_%s%d.ecs", exchangeId, snapshotId, type.code, instanceId));
    }

    private Path resolveMainLogPath() {
        return folder.resolve(String.format("%s.eca", exchangeId));
    }

    private Path resolveJournalPath(int partitionId) {
        return folder.resolve(String.format("%s_journal_%d_%d_%s_%04d.ecj", exchangeId, baseSnapshotId, partitionId, today, filesCounter));
    }
}
