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

import exchange.core2.core.ExchangeApi;
import exchange.core2.core.common.BalanceAdjustmentType;
import exchange.core2.core.common.OrderAction;
import exchange.core2.core.common.OrderType;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.common.config.ExchangeConfiguration;
import exchange.core2.core.common.config.InitialStateConfiguration;
import exchange.core2.core.common.config.PerformanceConfiguration;
import lombok.extern.slf4j.Slf4j;
import net.jpountz.lz4.*;
import net.jpountz.xxhash.XXHashFactory;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import net.openhft.chronicle.wire.InputStreamToWire;
import net.openhft.chronicle.wire.Wire;
import net.openhft.chronicle.wire.WireType;
import org.agrona.collections.MutableLong;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;


@Slf4j
public final class DiskSerializationProcessor implements ISerializationProcessor {

    private final int journalBufferFlushTrigger;
    private final long journalFileMaxSize;
    private final int journalBatchCompressThreshold;

    private final String exchangeId; // TODO validate
    private final Path folder;

    private final long baseSeq;

    private final ByteBuffer journalWriteBuffer;
    private final ByteBuffer lz4WriteBuffer;

    // TODO configurable
    private final LZ4Compressor lz4CompressorSnapshot;
    private final LZ4Compressor lz4CompressorJournal;
    private final LZ4SafeDecompressor lz4SafeDecompressor = LZ4Factory.fastestInstance().safeDecompressor();

    private ConcurrentSkipListMap<Long, SnapshotDescriptor> snapshotsIndex;

    private SnapshotDescriptor lastSnapshotDescriptor;
    private JournalDescriptor lastJournalDescriptor;


    private long baseSnapshotId;

    private long enableJournalAfterSeq = -1;

    private RandomAccessFile raf;
    private FileChannel channel;

    private int filesCounter = 0;

    private long writtenBytes = 0;

    private static final int MAX_COMMAND_SIZE_BYTES = 256;

//    private List<Integer> batchSizes = new ArrayList<>(100000);
//    final SingleWriterRecorder hdrRecorderRaw = new SingleWriterRecorder(Integer.MAX_VALUE, 2);
//    final SingleWriterRecorder hdrRecorderLz4 = new SingleWriterRecorder(Integer.MAX_VALUE, 2);

    public DiskSerializationProcessor(ExchangeConfiguration exchangeConfig,
                                      DiskSerializationProcessorConfiguration diskConfig) {

        final InitialStateConfiguration initStateCfg = exchangeConfig.getInitStateCfg();

        this.exchangeId = initStateCfg.getExchangeId();
        this.folder = Paths.get(diskConfig.getStorageFolder());
        this.baseSnapshotId = initStateCfg.getSnapshotId();
        this.baseSeq = initStateCfg.getSnapshotBaseSeq();

        final PerformanceConfiguration perfCfg = exchangeConfig.getPerformanceCfg();

        this.lastJournalDescriptor = null; // no journal
        this.lastSnapshotDescriptor = SnapshotDescriptor.createEmpty(perfCfg.getMatchingEnginesNum(), perfCfg.getRiskEnginesNum());

        final int journalBufferSize = diskConfig.getJournalBufferSize();

        this.journalFileMaxSize = diskConfig.getJournalFileMaxSize() - journalBufferSize;

        this.journalBufferFlushTrigger = journalBufferSize - MAX_COMMAND_SIZE_BYTES; // less than max command size in bytes
        this.journalBatchCompressThreshold = diskConfig.getJournalBatchCompressThreshold();

        this.journalWriteBuffer = ByteBuffer.allocateDirect(journalBufferSize);

        this.lz4CompressorJournal = diskConfig.getJournalLz4CompressorFactory().get();
        this.lz4CompressorSnapshot = diskConfig.getSnapshotLz4CompressorFactory().get();

        final int maxCompressedBlockLength = lz4CompressorJournal.maxCompressedLength(journalBufferSize);
        this.lz4WriteBuffer = ByteBuffer.allocate(maxCompressedBlockLength);
    }

    @Override
    public boolean storeData(long snapshotId,
                             long seq,
                             long timestampNs,
                             SerializedModuleType type,
                             int instanceId,
                             WriteBytesMarshallable obj) {

        final Path path = resolveSnapshotPath(snapshotId, type, instanceId);

        log.debug("Writing state into file {} ...", path);

        try (final OutputStream os = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW);
             final OutputStream bos = new BufferedOutputStream(os);
             final LZ4FrameOutputStream lz4os = new LZ4FrameOutputStream(
                     bos,
                     LZ4FrameOutputStream.BLOCKSIZE.SIZE_4MB,
                     -1,
                     lz4CompressorSnapshot,
                     XXHashFactory.fastestInstance().hash32(),
                     LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
             final WireToOutputStream2 wireToOutputStream = new WireToOutputStream2(WireType.RAW, lz4os)) {

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
            // TODO improve format
            try (final OutputStream os = Files.newOutputStream(resolveMainLogPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                os.write((System.currentTimeMillis() + " seq=" + seq + " timestampNs=" + timestampNs + " snapshotId=" + snapshotId + " type=" + type.code + " instance=" + instanceId + "\n").getBytes());
            } catch (final IOException ex) {
                log.error("Can not write main log file: ", ex);
                return false;
            }
        }

        return true;
    }

    @Override
    public <T> T loadData(long snapshotId,
                          SerializedModuleType type,
                          int instanceId,
                          Function<BytesIn, T> initFunc) {

        final Path path = resolveSnapshotPath(snapshotId, type, instanceId);

        log.debug("Loading state from {}", path);
        try (final InputStream is = Files.newInputStream(path, StandardOpenOption.READ);
             final InputStream bis = new BufferedInputStream(is);
             final LZ4FrameInputStream lz4is = new LZ4FrameInputStream(bis)) {

            // TODO improve reading algorithm
            final InputStreamToWire inputStreamToWire = new InputStreamToWire(WireType.RAW, lz4is);
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


    // single threaded
    @Override
    public void writeToJournal(OrderCommand cmd, long dSeq, boolean eob) throws IOException {

        // TODO improve checks logic
        // skip
        if (enableJournalAfterSeq == -1 || dSeq + baseSeq <= enableJournalAfterSeq) {
            return;
        }
        if (dSeq + baseSeq == enableJournalAfterSeq + 1) {
            log.info("Enabled journaling at seq = {} ({}+{})", enableJournalAfterSeq + 1, baseSeq, dSeq);
        }

        boolean debug = false;

//        log.debug("Writing {}", cmd);

        final OrderCommandType cmdType = cmd.command;

        if (cmdType == OrderCommandType.SHUTDOWN_SIGNAL) {
            flushBufferSync(false, cmd.timestamp);
            log.debug("Shutdown signal received, flushed to disk");
            return;
        }

        if (!cmdType.isMutate()) {
            // skip queries
            return;
        }

        if (channel == null) {
            startNewFile(cmd.timestamp);
        }

        final ByteBuffer buffer = journalWriteBuffer;

        // mandatory fields
        buffer.put(cmdType.getCode()); // 1 byte
        buffer.putLong(baseSeq + dSeq); // 8 bytes - can be compressed as delta
        buffer.putLong(cmd.timestamp); // 8 bytes - can be compressed as delta
        buffer.putInt(cmd.serviceFlags); // 4 bytes - can be compressed as dictionary
        buffer.putLong(cmd.eventsGroup); // 8 bytes - can be compressed as delta

        if (debug)
            log.debug("LOG {} eventsGroup={} serviceFlags={}", String.format("seq=%d t=%d cmd=%X (%s) ", baseSeq + dSeq, cmd.timestamp, cmdType.getCode(), cmdType), cmd.eventsGroup, cmd.serviceFlags);

        if (cmdType == OrderCommandType.MOVE_ORDER) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary
            buffer.putInt(cmd.symbol); // 4 bytes can be compressed as dictionary
            buffer.putLong(cmd.orderId); // 8 bytes - can be compressed as delta
            buffer.putLong(cmd.price); // 8 bytes - can be compressed as delta

            if (debug)
                log.debug("move order seq={} t={} orderId={} symbol={} uid={} price={}", baseSeq + dSeq, cmd.timestamp, cmd.orderId, cmd.symbol, cmd.uid, cmd.price);

        } else if (cmdType == OrderCommandType.CANCEL_ORDER) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary
            buffer.putInt(cmd.symbol); // 4 bytes can be compressed as dictionary
            buffer.putLong(cmd.orderId); // 8 bytes - can be compressed as delta

            if (debug)
                log.debug("cancel order seq={} t={} orderId={} symbol={} uid={}", baseSeq + dSeq, cmd.timestamp, cmd.orderId, cmd.symbol, cmd.uid);

        } else if (cmdType == OrderCommandType.REDUCE_ORDER) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary
            buffer.putInt(cmd.symbol); // 4 bytes can be compressed as dictionary
            buffer.putLong(cmd.orderId); // 8 bytes - can be compressed as delta
            buffer.putLong(cmd.size); // 8 bytes - can be compressed as low value

            if (debug)
                log.debug("reduce order seq={} t={} orderId={} symbol={} uid={} size={}", baseSeq + dSeq, cmd.timestamp, cmd.orderId, cmd.symbol, cmd.uid, cmd.size);

        } else if (cmdType == OrderCommandType.PLACE_ORDER) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary
            buffer.putInt(cmd.symbol); // 4 bytes can be compressed as dictionary
            buffer.putLong(cmd.orderId); // 8 bytes - can be compressed as delta
            buffer.putLong(cmd.price); // 8 bytes - can be compressed as delta
            buffer.putLong(cmd.reserveBidPrice); // 8 bytes - can be compressed (diff to price or 0)
            buffer.putLong(cmd.size); // 8 bytes - can be compressed
            buffer.putLong(cmd.orderTakerFee); // 4 bytes
            buffer.putLong(cmd.orderMakerFee); // 4 bytes
            buffer.putInt(cmd.userCookie); // 4 bytes can be log-compressed

            final int actionAndType = (cmd.orderType.getCode() << 1) | cmd.action.getCode();
            byte actionAndType1 = (byte) actionAndType;
            buffer.put(actionAndType1); // 1 byte

            if (debug)
                log.debug("place order seq={} t={} orderId={} symbol={} uid={} price={} reserveBidPrice={} size={} userCookie={} {}/{} actionAndType={}",
                        baseSeq + dSeq, cmd.timestamp, cmd.orderId, cmd.symbol, cmd.uid, cmd.price, cmd.reserveBidPrice, cmd.size, cmd.userCookie, cmd.action, cmd.orderType, actionAndType1);

        } else if (cmdType == OrderCommandType.BALANCE_ADJUSTMENT) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary
            buffer.putInt(cmd.symbol); // 4 bytes can be compressed as dictionary (currency)
            buffer.putLong(cmd.orderId); // 8 bytes can be compressed as delta (transaction)
            buffer.putLong(cmd.price); // 8 bytes - can be compressed as low value (amount)
            buffer.put(cmd.orderType.getCode()); // 1 byte (adjustment or suspend)

        } else if (cmdType == OrderCommandType.ADD_USER ||
                cmdType == OrderCommandType.SUSPEND_USER ||
                cmdType == OrderCommandType.RESUME_USER) {

            buffer.putLong(cmd.uid); // 8 bytes can be compressed as delta

        } else if (cmdType == OrderCommandType.BINARY_DATA_COMMAND) {

//            if (debug) log.debug("LOG BINARY_DATA_COMMAND {}", String.format("seq=%d f=%d word0=%X word1=%X word2=%X word3=%X word4=%X",
//                    dSeq + baseSeq, (byte) cmd.symbol, cmd.orderId, cmd.price, cmd.reserveBidPrice, cmd.size, cmd.uid));

            buffer.put((byte) cmd.symbol); // 1 byte (0 or -1)
            buffer.putLong(cmd.orderId); // 8 bytes word0
            buffer.putLong(cmd.price); // 8 bytes word1
            buffer.putLong(cmd.reserveBidPrice); // 8 bytes word2
            buffer.putLong(cmd.size); // 8 bytes word3
            buffer.putLong(cmd.uid); // 8 bytes word4

//        } else if (cmdType == OrderCommandType.PERSIST_STATE_MATCHING ||
//                cmdType == OrderCommandType.PERSIST_STATE_RISK) {
//            buffer.putLong(cmd.orderId); // 8 bytes
        }

        if (cmdType == OrderCommandType.PERSIST_STATE_RISK) {

            // register snapshot change
            registerNextSnapshot(cmd.orderId, baseSeq + dSeq, cmd.timestamp);

            // start new file
            baseSnapshotId = cmd.orderId;
            filesCounter = 0;

            flushBufferSync(true, cmd.timestamp);

        } else if (cmdType == OrderCommandType.RESET) {

            // forcing to start next journal file on reset (useful for testing)
            flushBufferSync(true, cmd.timestamp);

        } else if (eob || buffer.position() >= journalBufferFlushTrigger) {

            // flushing on end of batch or when buffer is full
            flushBufferSync(false, cmd.timestamp);
        }

    }

    @Override
    public void enableJournaling(long afterSeq, ExchangeApi api) {
        enableJournalAfterSeq = afterSeq;
        api.groupingControl(0, 1);
    }

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for (byte b : a)
            sb.append(String.format("%02x ", b));
        return sb.toString();
    }

    @Override
    public NavigableMap<Long, SnapshotDescriptor> findAllSnapshotPoints() {
        return snapshotsIndex;
    }

    @Override
    public void replayJournalStep(long snapshotId, long seqFrom, long seqTo, ExchangeApi exchangeApi) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long replayJournalFull(InitialStateConfiguration initialCfg, ExchangeApi api) {
        if (initialCfg.getJournalTimestampNs() == 0) {
            log.debug("No need to replay journal, returning baseSeq={}", baseSeq);

            return baseSeq;
        }
        log.debug("Replaying journal...");

//        log.info("Read total: {} bytes ", totalBytesRead);

        api.groupingControl(0, 0);


        final MutableLong lastSeq = new MutableLong();
        // TODO refactor reading, use EOF flag

        int partitionCounter = 1;
        while (true) {

            final Path path = resolveJournalPath(partitionCounter, initialCfg.getSnapshotId());

            log.debug("Reading journal file: {}", path.toFile());
            try (final FileInputStream fis = new FileInputStream(path.toFile());
                 final BufferedInputStream bis = new BufferedInputStream(fis);
                 final DataInputStream dis = new DataInputStream(bis)) {

                readCommands(dis, api, lastSeq, false);
                partitionCounter++;
                log.debug("File end reached, try next partition {}...", partitionCounter);

            } catch (FileNotFoundException ex) {
                log.debug("return lastSeq={}, file not found: {}", lastSeq, ex.getMessage());
                return lastSeq.value;

            } catch (IOException ex) {
                partitionCounter++;
                log.debug("File end reached through exception");
            }
        }
    }


    private void readCommands(final DataInputStream jr,
                              final ExchangeApi api,
                              final MutableLong lastSeq,
                              boolean insideCompressedBlock) throws IOException {

        while (jr.available() != 0) {

            boolean debug = false;
//            boolean debug = insideCompressedBlock;

            final byte cmd = jr.readByte();

            if (debug) log.debug("COMPR STEP lastSeq={} ", lastSeq);

            if (cmd == OrderCommandType.RESERVED_COMPRESSED.getCode()) {

                if (insideCompressedBlock) {
                    throw new IllegalStateException("Recursive compression block (data corrupted)");
                }

                int size = jr.readInt();
                int origSize = jr.readInt();

//                log.debug("{}->{}", size, origSize);

                if (size > 1000000) {
                    throw new IllegalStateException("Bad compressed block size = " + size + "(data corrupted)");
                }

                if (origSize > 1000000) {
                    throw new IllegalStateException("Bad original block size = " + size + "(data corrupted)");
                }

                byte[] compressedArray = new byte[size];
                int read = jr.read(compressedArray);
                if (read < size) {
                    throw new IOException("Can not read full block (only " + read + " bytes, not all " + size + " bytes) ");
                }

//                log.debug("Decoding block {}", origSize);
                byte[] originalArray = lz4SafeDecompressor.decompress(compressedArray, origSize);

                // read compressed block recursively
                try (final ByteArrayInputStream bis = new ByteArrayInputStream(originalArray);
                     final DataInputStream dis = new DataInputStream(bis)) {

                    readCommands(dis, api, lastSeq, true);
                }

            } else {

                final long seq = jr.readLong();

                final long timestampNs = jr.readLong();
                final int serviceFlags = jr.readInt();
                final long eventsGroup = jr.readLong();
                final OrderCommandType cmdType = OrderCommandType.fromCode(cmd);

                if (seq != lastSeq.value + 1) {
                    log.warn("Sequence gap {}->{} ({})", lastSeq, seq, seq - lastSeq.value);
//                    log.debug("timestampNs={} eventsGroup={} serviceFlags={} cmdType={}", timestampNs, eventsGroup, serviceFlags, cmdType);
                }

                lastSeq.value = seq;

//                log.debug("command seq={} {}", lastSeq, cmdType);

                if (debug) log.debug("eventsGroup={} serviceFlags={} cmdType={}", eventsGroup, serviceFlags, cmdType);

                if (cmdType == OrderCommandType.MOVE_ORDER) {

                    final long uid = jr.readLong(); // 8 bytes can be compressed as dictionary
                    final int symbol = jr.readInt();// 4 bytes can be compressed as dictionary
                    final long orderId = jr.readLong(); // 8 bytes - can be compressed as delta
                    final long price = jr.readLong(); // 8 bytes - can be compressed as delta

                    if (debug)
                        log.debug("move order seq={} t={} orderId={} symbol={} uid={} price={}", lastSeq, timestampNs, orderId, symbol, uid, price);

                    api.moveOrder(serviceFlags, eventsGroup, timestampNs, price, orderId, symbol, uid);

                } else if (cmdType == OrderCommandType.CANCEL_ORDER) {

                    final long uid = jr.readLong(); // 8 bytes can be compressed as dictionary
                    final int symbol = jr.readInt();// 4 bytes can be compressed as dictionary
                    final long orderId = jr.readLong(); // 8 bytes - can be compressed as delta

                    if (debug)
                        log.debug("cancel order seq={} t={} orderId={} symbol={} uid={}", lastSeq, timestampNs, orderId, symbol, uid);

                    api.cancelOrder(serviceFlags, eventsGroup, timestampNs, orderId, symbol, uid);

                } else if (cmdType == OrderCommandType.REDUCE_ORDER) {

                    final long uid = jr.readLong(); // 8 bytes can be compressed as dictionary
                    final int symbol = jr.readInt();// 4 bytes can be compressed as dictionary
                    final long orderId = jr.readLong(); // 8 bytes - can be compressed as delta
                    final long reduceSize = jr.readLong(); // 8 bytes - can be compressed as low value

                    if (debug)
                        log.debug("reduce order seq={} t={} orderId={} symbol={} uid={} reduceSize={}", lastSeq, timestampNs, orderId, symbol, uid, reduceSize);

                    api.reduceOrder(serviceFlags, eventsGroup, timestampNs, reduceSize, orderId, symbol, uid);

                } else if (cmdType == OrderCommandType.PLACE_ORDER) {

                    final long uid = jr.readLong(); // 8 bytes can be compressed as dictionary
                    final int symbol = jr.readInt();// 4 bytes can be compressed as dictionary
                    final long orderId = jr.readLong(); // 8 bytes - can be compressed as delta
                    final long price = jr.readLong(); // 8 bytes - can be compressed as delta
                    final long reservedBidPrice = jr.readLong(); // 8 bytes - can be compressed (diff to price or 0)
                    final long size = jr.readLong(); // 8 bytes - can be compressed
                    final long orderTakerFee = jr.readInt();
                    final int orderMakerFee = jr.readInt();
                    final int userCookie = jr.readInt(); // 4 bytes can be compressed as a optional low value

                    final byte actionAndType = jr.readByte(); // 1 byte
                    final OrderAction orderAction = OrderAction.of((byte) (actionAndType & 0b1));
                    final OrderType orderType = OrderType.of((byte) ((actionAndType >> 1) & 0b1111));

                    if (debug)
                        log.debug("place order seq={} t={} orderId={} symbol={} uid={} price={} reserveBidPrice={} size={} userCookie={} {}/{} actionAndType={}", lastSeq, timestampNs, orderId, symbol, uid, price, reservedBidPrice, size, userCookie, orderAction, orderType, actionAndType);

                    api.placeNewOrder(serviceFlags, eventsGroup, timestampNs, orderId, userCookie, price,
                            reservedBidPrice, size, orderAction, orderType, symbol, uid, orderTakerFee, orderMakerFee);

                } else if (cmdType == OrderCommandType.BALANCE_ADJUSTMENT) {

                    final long uid = jr.readLong(); // 8 bytes can be compressed as dictionary
                    final int currency = jr.readInt();// 4 bytes can be compressed as dictionary (currency)
                    final long transactionId = jr.readLong(); // 8 bytes can be compressed as delta (transaction)
                    final long amount = jr.readLong(); // 8 bytes - can be compressed as low value (amount)
                    final BalanceAdjustmentType adjustmentType = BalanceAdjustmentType.of(jr.readByte()); // 1 byte (adjustment or suspend)

                    if (debug)
                        log.debug("balanceAdjustment seq={}  {} uid:{} curre:{}", lastSeq, timestampNs, uid, currency);

                    api.balanceAdjustment(serviceFlags, eventsGroup, timestampNs, uid, transactionId, currency, amount, adjustmentType);

                } else if (cmdType == OrderCommandType.ADD_USER) {

                    final long uid = jr.readLong(); // 8 bytes can be compressed as dictionary

                    if (debug) log.debug("add user  seq={}  {} uid:{} ", lastSeq, timestampNs, uid);

                    api.createUser(serviceFlags, eventsGroup, timestampNs, uid);

                } else if (cmdType == OrderCommandType.SUSPEND_USER) {

                    final long uid = jr.readLong(); // 8 bytes can be compressed as dictionary

                    if (debug) log.debug("suspend user seq={}  {} uid:{} ", lastSeq, timestampNs, uid);

                    api.suspendUser(serviceFlags, eventsGroup, timestampNs, uid);

                } else if (cmdType == OrderCommandType.RESUME_USER) {

                    final long uid = jr.readLong(); // 8 bytes can be compressed as dictionary

                    if (debug) log.debug("resume user seq={}  {} uid:{} ", lastSeq, timestampNs, uid);

                    api.resumeUser(serviceFlags, eventsGroup, timestampNs, uid);

                } else if (cmdType == OrderCommandType.BINARY_DATA_COMMAND) {

                    final byte lastFlag = jr.readByte(); // 1 byte (0 or -1)
                    final long word0 = jr.readLong(); // 8 bytes word0
                    final long word1 = jr.readLong(); // 8 bytes word1
                    final long word2 = jr.readLong(); // 8 bytes word2
                    final long word3 = jr.readLong(); // 8 bytes word3
                    final long word4 = jr.readLong(); // 8 bytes word4

                    if (debug)
                        log.debug("binary data seq={} t:{} {}", lastSeq, timestampNs, String.format("f=%d word0=%X word1=%X word2=%X word3=%X word4=%X", lastFlag, word0, word1, word2, word3, word4));

                    api.binaryData(serviceFlags, eventsGroup, timestampNs, lastFlag, word0, word1, word2, word3, word4);

                } else if (cmdType == OrderCommandType.RESET) {

                    api.reset(timestampNs);

                } else {

                    log.debug("eventsGroup={} serviceFlags={} cmdType={}", eventsGroup, serviceFlags, cmdType);

                    throw new IllegalStateException("unexpected command");
                }
            }
        }

    }


    @Override
    public void replayJournalFullAndThenEnableJouraling(InitialStateConfiguration initialStateConfiguration, ExchangeApi exchangeApi) {
        long seq = replayJournalFull(initialStateConfiguration, exchangeApi);
        enableJournaling(seq, exchangeApi);
    }

    @Override
    public boolean checkSnapshotExists(long snapshotId, SerializedModuleType type, int instanceId) {
        final Path path = resolveSnapshotPath(snapshotId, type, instanceId);
        final boolean exists = Files.exists(path);
        log.info("Checking snapshot file {} exists:{}", path, exists);
        return exists;
    }

    private void flushBufferSync(final boolean forceStartNextFile, final long timestampNs) throws IOException {

//        log.debug("Flushing buffer position={}", buffer.position());

//        batchSizes.add(journalWriteBuffer.position());
//        if (batchSizes.size() == 1000) {
//            log.debug("Journal average batchSize = {} bytes", batchSizes.stream().mapToInt(c -> c).average());
//            batchSizes = new ArrayList<>();
//        }

        if (journalWriteBuffer.position() < journalBatchCompressThreshold) {
            // uncompressed write for single messages or small batches
            writtenBytes += journalWriteBuffer.position();
            journalWriteBuffer.flip();
//            long t = System.nanoTime();
            channel.write(journalWriteBuffer);
//            hdrRecorderRaw.recordValue(System.nanoTime() - t);
            journalWriteBuffer.clear();

        } else {
            // compressed write for bigger batches
//            long t = System.nanoTime();
            int originalLength = journalWriteBuffer.position(); // commands code
            journalWriteBuffer.flip();
            lz4WriteBuffer.put(OrderCommandType.RESERVED_COMPRESSED.getCode()); // compressed block
            lz4WriteBuffer.putInt(0); // reserve space
            lz4WriteBuffer.putInt(0); // reserve space
            lz4CompressorJournal.compress(journalWriteBuffer, lz4WriteBuffer);
            journalWriteBuffer.clear();
            writtenBytes += lz4WriteBuffer.position();
            int remainingCompressedLength = lz4WriteBuffer.position() - 9; // 1 + 4 + 4
            lz4WriteBuffer.putInt(1, remainingCompressedLength); // 1 byte offset
            lz4WriteBuffer.putInt(5, originalLength); // 1 + 4 bytes offset
            lz4WriteBuffer.flip();
//            hdrRecorderLz4.recordValue(System.nanoTime() - t);
            channel.write(lz4WriteBuffer);
            lz4WriteBuffer.clear();
        }

        if (forceStartNextFile || writtenBytes >= journalFileMaxSize) {

//            log.info("RAW {}", LatencyTools.createLatencyReportFast(hdrRecorderRaw.getIntervalHistogram()));
//            log.info("LZ4-compression {}", LatencyTools.createLatencyReportFast(hdrRecorderLz4.getIntervalHistogram()));

            // todo start preparing new file asynchronously, but ONLY ONCE
            startNewFile(timestampNs);
            writtenBytes = 0;
        }
    }

    private void startNewFile(final long timestampNs) throws IOException {
        filesCounter++;
        if (channel != null) {
            channel.close();
            raf.close();
        }
        final Path fileName = resolveJournalPath(filesCounter, baseSnapshotId);
//        log.debug("Starting new journal file: {}", fileName);

        if (Files.exists(fileName)) {
            throw new IllegalStateException("File already exists: " + fileName);
        }

        raf = new RandomAccessFile(fileName.toString(), "rwd");
        channel = raf.getChannel();

        registerNextJournal(baseSnapshotId, timestampNs); // TODO fix time
    }

    /**
     * call only from journal thread
     *
     * @param seq
     * @param timestampNs
     */
    private void registerNextJournal(long seq, long timestampNs) {

        lastJournalDescriptor = new JournalDescriptor(timestampNs, seq, lastSnapshotDescriptor, lastJournalDescriptor);
    }


    /**
     * call only from journal thread
     *
     * @param snapshotId
     * @param seq
     * @param timestampNs
     */
    private void registerNextSnapshot(long snapshotId,
                                      long seq,
                                      long timestampNs) {

        lastSnapshotDescriptor = lastSnapshotDescriptor.createNext(snapshotId, seq, timestampNs);
    }

    private Path resolveSnapshotPath(long snapshotId, SerializedModuleType type, int instanceId) {

        return folder.resolve(String.format("%s_snapshot_%d_%s%d.ecs", exchangeId, snapshotId, type.code, instanceId));
    }

    private Path resolveMainLogPath() {
        return folder.resolve(String.format("%s.eca", exchangeId));
    }

    private Path resolveJournalPath(int partitionId, long snapshotId) {
        return folder.resolve(String.format("%s_journal_%d_%04X.ecj", exchangeId, snapshotId, partitionId));
    }
}
