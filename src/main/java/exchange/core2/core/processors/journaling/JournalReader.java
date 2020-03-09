package exchange.core2.core.processors.journaling;

import lombok.extern.slf4j.Slf4j;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

@Slf4j
public class JournalReader implements AutoCloseable {


    private static final int BUFFER_SIZE = 64 * 1024;

    private final byte[] buffer = new byte[BUFFER_SIZE];
    private final ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);

    private final FileInputStream fis;
    private final BufferedInputStream bis;

    private int bufferPointer = 0;
    private int bufferEnd;


    // TODO extra layer (InputStream) ?

    public JournalReader(Path path) throws IOException {
        log.debug("Reading journal file: {}", path.toFile());
        this.fis = new FileInputStream(path.toFile());
        this.bis = new BufferedInputStream(fis);
        bufferEnd = bis.read(buffer);
//        log.debug("Read buffer: bufferEnd={} {}", bufferEnd, byteArrayToHex(buffer));
    }


    public byte readByte() throws IOException {

        if (bufferPointer == bufferEnd) {
            readBuffer();
        }
        byte b = buffer[bufferPointer++];
//        log.debug("byte = {}", String.format("%02X", b));

        return b;
    }

    public int readInt() throws IOException {

        if (bufferPointer + 4 > bufferEnd) {
            readBuffer();
        }

        int anInt = byteBuffer.getInt(bufferPointer);

        bufferPointer += 4;

//        log.debug("int  = {}", String.format("%08X", anInt));
        return anInt;
    }

    public long readLong() throws IOException {

        if (bufferPointer + 8 > bufferEnd) {
            readBuffer();
        }

        long aLong = byteBuffer.getLong(bufferPointer);
        bufferPointer += 8;
//        log.debug("long = {}", String.format("%016X", aLong));
        return aLong;
    }

    private void readBuffer() throws IOException {

        bufferEnd = BUFFER_SIZE - bufferPointer;
        if (bufferEnd != 0) {
//            log.debug("Read buffer move: bufferEnd={} = BUFFER_SIZE={} - bufferPointer={}", bufferEnd, BUFFER_SIZE, bufferPointer);
            // copy bytes left to the beginning of the buffer
            System.arraycopy(buffer, bufferPointer, buffer, 0, bufferEnd);
        }
        int read = bis.read(buffer, bufferEnd, bufferPointer);
        if (read <= 0) {
            throw new IOException("Can not read more data");
        }

        bufferEnd += read;
//        log.debug("Read buffer: read={} {}", read, byteArrayToHex(buffer));
        bufferPointer = 0;
    }

    @Override
    public void close() throws IOException {
        bis.close();
        fis.close();
    }
}
