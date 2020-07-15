package exchange.core2.core.processors;

import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import lombok.extern.slf4j.Slf4j;
import sun.misc.Contended;

@Slf4j
public final class GroupingHelper {

    private final long timeWindowNs = 20_000; // TODO use maxGroupDurationNs
    private final long frameSize = 256; // TODO use msgsInGroupLimit

    @Contended
    private long seqThreshold = -1;

    @Contended
    private long timeThreshold = Long.MIN_VALUE;

    private long x = 0;

    public boolean nextEvent(final long seq, final OrderCommand cmd) {


//        if ((seq & 0x7FFFFF) == 0) {
//            log.debug("numFrames={} sizeAvg={}", x, seq / (x + 1));
//        }

        // TODO cmd.timestamp - is it correct to use planned timestamp?

        // Rb flush by time, ser, or special command
        if (cmd.timestamp > timeThreshold || seq > seqThreshold || (cmd.command.getCode() >= 50 && checkSpecialCommand(cmd))) {
//        if ( seq > seqThreshold || (cmd.command.getCode() >= 50 && checkSpecialCommand(cmd))) {

            // TODO   avoid changing groups when PERSIST_STATE_MATCHING is already executing

            timeThreshold = cmd.timestamp + timeWindowNs;
            seqThreshold = seq + frameSize;


//            x++;
            return true;
        }

        // TODO serviceFlags

        return false;
    }

    private boolean checkSpecialCommand(OrderCommand cmd) {
        // report/binary commands also should trigger Rb flush, but only for last message
        if ((cmd.command == OrderCommandType.BINARY_DATA_COMMAND || cmd.command == OrderCommandType.BINARY_DATA_QUERY) && cmd.symbol != -1) {
            return false;
        } else if (cmd.command == OrderCommandType.GROUPING_CONTROL) {
            throw new UnsupportedOperationException(); // TODO implement (should return SUCCESS)
        } else {
            return true;
        }
    }


}
