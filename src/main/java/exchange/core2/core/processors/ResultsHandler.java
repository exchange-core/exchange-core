package exchange.core2.core.processors;

import com.lmax.disruptor.EventHandler;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;

import java.util.function.ObjLongConsumer;

public final class ResultsHandler implements EventHandler<OrderCommand> {

    private final ObjLongConsumer<OrderCommand> resultsConsumer;

    private boolean processingEnabled = true;

    @Override
    public void onEvent(OrderCommand cmd, long sequence, boolean endOfBatch) {

        if (cmd.command == OrderCommandType.GROUPING_CONTROL) {
            processingEnabled = cmd.orderIdLo == 1;
        }

        if (processingEnabled) {
            resultsConsumer.accept(cmd, sequence);
        }

    }

    public ResultsHandler(ObjLongConsumer<OrderCommand> resultsConsumer) {
        this.resultsConsumer = resultsConsumer;
    }
}
