package org.openpredict.exchange.core;

import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Journal writer
 * <p>
 * - stateful handler
 * - not thread safe!
 */
@Slf4j
public class JournallingProcessor implements EventHandler<OrderCommand> {

    private static final int MB = 1024 * 1024;
    private static final int FILE_SIZE_TRIGGER = 1024 * MB; // split files by size
    private static final String FILE_NAME_PATTERN = "/exchange/data/%s_%04d.olog";
    private static final String DATE_FORMAT = "yyyy-MM-dd_HHmmss";

    private static final int BUFFER_SIZE = 65536;
    private static final int BUFFER_FLUSH_TRIGER = BUFFER_SIZE - 256;

    private RandomAccessFile raf;
    private ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
    private int filesCounter = 0;

    private long writtenBytes = 0;

    private final String today = LocalDateTime.now().format(DateTimeFormatter.ofPattern(DATE_FORMAT));

    // TODO asynchronously create new file and then switch reference

    @Override
    public void onEvent(OrderCommand cmd, long seq, boolean eob) throws Exception {

//        log.debug("Writing {}", cmd);

        //buffer.putInt(cmd.symbol); // TODO Header

        // TODO allocate big buffer, just move pointer

        // 25 bytes
        buffer.putLong(cmd.timestamp); // 8 bytes
        buffer.put(cmd.command.getCode()); // 1 byte
        buffer.putLong(cmd.orderId); // 8 bytes - can be compressed as delta
        buffer.putLong(cmd.uid); // 8 bytes can be compressed as dictionary

        // 12 bytes
        if (cmd.command == OrderCommandType.MOVE_ORDER || cmd.command == OrderCommandType.PLACE_ORDER) {
            buffer.putLong(cmd.price); // 8 bytes - can be compressed as delta
            buffer.putLong(cmd.size); // 8 bytes - can be compressed
        }

        // 1 byte
        if (cmd.command == OrderCommandType.PLACE_ORDER) {
            int actionAndType = (cmd.action.getCode() << 2) & cmd.orderType.getCode();
            buffer.put((byte) actionAndType); // 1 byte
        }

        if (eob || buffer.position() >= BUFFER_FLUSH_TRIGER) {
//            log.debug("Flushing {} bytes", buffer.position());
            flushBufferSync();
        }
    }

    private void flushBufferSync() throws IOException {
        raf.write(buffer.array(), 0, buffer.position());
        writtenBytes += buffer.position();
        buffer.clear();

        if (writtenBytes >= FILE_SIZE_TRIGGER) {
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
        raf = new RandomAccessFile(String.format(FILE_NAME_PATTERN, today, filesCounter), "rwd");
    }
}
