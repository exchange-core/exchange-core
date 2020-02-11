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
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Journal writer
 * <p>
 * - stateful handler
 * - not thread safe!
 */
@Slf4j
public final class DiskJournalingProcessor implements IJournalingProcessor {

    private static final long MB = 1024 * 1024;
    private static final long FILE_SIZE_TRIGGER = 4 * 1024 * MB; // split files by size
    private static final String FILE_NAME_PATTERN = "%s_%04d.eclog";
    private static final String DATE_FORMAT = "yyyy-MM-dd_HHmmss";

    private static final int BUFFER_SIZE = 128 * 1024;
    private static final int BUFFER_FLUSH_TRIGER = BUFFER_SIZE - 256;


    private final Path journalFolder;


    private long baseSnapshotId;

    private RandomAccessFile raf;
    private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private int filesCounter = 0;

    private long writtenBytes = 0;

    private final String today = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DATE_FORMAT));


    public DiskJournalingProcessor(final Path journalFolder, final long baseSnapshotId) {
        this.journalFolder = journalFolder;
        this.baseSnapshotId = baseSnapshotId;
    }

    public DiskJournalingProcessor(final String journalFolder, final long baseSnapshotId) {
        this.journalFolder = Paths.get(journalFolder);
        this.baseSnapshotId = baseSnapshotId;
    }


    // TODO asynchronously create new file and then switch reference

    @Override
    public void onEvent(OrderCommand cmd, long seq, boolean eob) throws IOException {

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

        } else if (eob || buffer.position() >= BUFFER_FLUSH_TRIGER) {
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
        raf = new RandomAccessFile(resolvePath(filesCounter).toString(), "rwd");
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


    private Path resolvePath(int partitionId) {
        return journalFolder.resolve("journal_" + baseSnapshotId + "_" + partitionId + String.format(FILE_NAME_PATTERN, today, filesCounter));
    }
}
