package org.openpredict.exchange.core;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.NativeBytes;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.openpredict.exchange.beans.*;
import org.openpredict.exchange.beans.cmd.CommandResultCode;
import org.openpredict.exchange.beans.cmd.OrderCommand;
import org.openpredict.exchange.beans.cmd.OrderCommandType;
import org.openpredict.exchange.beans.reports.ReportQuery;
import org.openpredict.exchange.beans.reports.SingleUserReportQuery;
import org.openpredict.exchange.beans.reports.SingleUserReportResult;
import org.openpredict.exchange.beans.reports.TotalCurrencyBalanceReportResult;
import org.openpredict.exchange.core.journalling.ISerializationProcessor;
import org.openpredict.exchange.core.orderbook.IOrderBook;
import org.openpredict.exchange.core.orderbook.OrderBookEventsHelper;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static net.openhft.chronicle.core.UnsafeMemory.UNSAFE;
import static org.openpredict.exchange.beans.cmd.OrderCommandType.*;
import static org.openpredict.exchange.core.Utils.OFFSET_ORDER_ID;

@Slf4j
public final class MatchingEngineRouter implements WriteBytesMarshallable, StateHash {

    // state
    private final BinaryCommandsProcessor binaryCommandsProcessor;

    // symbol->OB
    private final IntObjectHashMap<IOrderBook> orderBooks;

    private final Function<CoreSymbolSpecification, IOrderBook> orderBookFactory;

    private final int shardId;
    private final long shardMask;

    private final ISerializationProcessor serializationProcessor;

    public MatchingEngineRouter(final int shardId,
                                final long numShards,
                                final ISerializationProcessor serializationProcessor,
                                final Function<CoreSymbolSpecification, IOrderBook> orderBookFactory,
                                final Long loadStateId) {

        if (Long.bitCount(numShards) != 1) {
            throw new IllegalArgumentException("Invalid number of shards " + numShards + " - must be power of 2");
        }
        this.shardId = shardId;
        this.shardMask = numShards - 1;
        this.serializationProcessor = serializationProcessor;
        this.orderBookFactory = orderBookFactory;

        if (loadStateId != null) {
            final Pair<BinaryCommandsProcessor, IntObjectHashMap<IOrderBook>> deserialized = serializationProcessor.loadData(
                    loadStateId,
                    ISerializationProcessor.SerializedModuleType.MATCHING_ENGINE_ROUTER,
                    shardId,
                    bytesIn -> {
                        if (shardId != bytesIn.readInt()) {
                            throw new IllegalStateException("wrong shardId");
                        }
                        if (shardMask != bytesIn.readLong()) {
                            throw new IllegalStateException("wrong shardMask");
                        }
                        final BinaryCommandsProcessor bcp = new BinaryCommandsProcessor(this::handleBinaryMessage, bytesIn, shardId + 1024);
                        final IntObjectHashMap<IOrderBook> ob = Utils.readIntHashMap(bytesIn, IOrderBook::create);
                        return Pair.of(bcp, ob);
                    });

            this.binaryCommandsProcessor = deserialized.getLeft();
            this.orderBooks = deserialized.getRight();

        } else {
            this.binaryCommandsProcessor = new BinaryCommandsProcessor(this::handleBinaryMessage, shardId + 1024);
            this.orderBooks = new IntObjectHashMap<>();
        }
    }

    public void processOrder(OrderCommand cmd) {

        final OrderCommandType command = cmd.command;

        if (command == MOVE_ORDER || command == CANCEL_ORDER || command == ORDER_BOOK_REQUEST || command == PLACE_ORDER) {
            // process specific symbol group only
            if (symbolForThisHandler(cmd.symbol)) {
                processMatchingCommand(cmd);
            }
        } else if (command == BINARY_DATA) {

            final boolean isLastFrame = binaryCommandsProcessor.acceptBinaryFrame(cmd);
            if (shardId == 0) {
                cmd.resultCode = isLastFrame ? CommandResultCode.SUCCESS : CommandResultCode.ACCEPTED;
            }

        } else if (command == RESET) {
            // process all symbols groups, only processor 0 writes result
            orderBooks.clear();
            binaryCommandsProcessor.reset();
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }

        } else if (command == PERSIST_STATE_MATCHING) {
            final boolean isSuccess = serializationProcessor.storeData(cmd.orderId, ISerializationProcessor.SerializedModuleType.MATCHING_ENGINE_ROUTER, shardId, this);
            // Send ACCEPTED because this is a first command in series. Risk engine is second - so it will return SUCCESS
            Utils.setResultVolatile(cmd, isSuccess, CommandResultCode.ACCEPTED, CommandResultCode.STATE_PERSIST_MATCHING_ENGINE_FAILED);

        } else if (command == STATE_HASH_REQUEST) {
            // common hash as sum of each module hash (for simplicity)
            UNSAFE.getAndAddLong(cmd, OFFSET_ORDER_ID, stateHash());
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }
        }

    }


    private Optional<? extends WriteBytesMarshallable> handleBinaryMessage(Object message) {
        if (message instanceof CoreSymbolSpecification) {
            // TODO return status object
            addSymbol((CoreSymbolSpecification) message);
            return Optional.empty();
        } else if (message instanceof ReportQuery) {
            return processReport((ReportQuery) message);
        } else {
            return Optional.empty();
        }
    }


    private Optional<? extends WriteBytesMarshallable> processReport(ReportQuery reportQuery) {

        switch (reportQuery.getReportType()) {

            case SINGLE_USER_REPORT:
                return reportSingleUser((SingleUserReportQuery) reportQuery);


            case TOTAL_CURRENCY_BALANCE:
                return reportGlobalBalance();

            default:
                throw new IllegalStateException("Report not implemented");
        }
    }

    private Optional<SingleUserReportResult> reportSingleUser(final SingleUserReportQuery query) {
        final LongObjectHashMap<Order> orders = new LongObjectHashMap<>();
        orderBooks.stream()
                .flatMap(ob -> ob.findUserOrders(query.getUid()).stream())
                .forEach(order -> orders.put(order.orderId, order));

        //log.debug("orders: {}", orders.size());
        return Optional.of(new SingleUserReportResult(null, orders, SingleUserReportResult.ExecutionStatus.OK));
    }

    private Optional<TotalCurrencyBalanceReportResult> reportGlobalBalance() {

        final IntLongHashMap currencyBalance = new IntLongHashMap();

        orderBooks.stream()
                .filter(ob -> ob.getSymbolSpec().type == SymbolType.CURRENCY_EXCHANGE_PAIR)
                .forEach(ob -> {
                    final CoreSymbolSpecification spec = ob.getSymbolSpec();

                    currencyBalance.addToValue(
                            spec.getBaseCurrency(),
                            ob.askOrdersStream(false).mapToLong(ord -> Utils.calculateAmountAsk(ord.size - ord.filled, spec)).sum());

                    currencyBalance.addToValue(
                            spec.getQuoteCurrency(),
                            ob.bidOrdersStream(false).mapToLong(ord -> Utils.calculateAmountBid(ord.size - ord.filled, ord.reserveBidPrice, spec)).sum());
                });

        return Optional.of(new TotalCurrencyBalanceReportResult(null, currencyBalance));
    }


    private boolean symbolForThisHandler(final long symbol) {
        return (shardMask == 0) || ((symbol & shardMask) == shardId);
    }


    private CommandResultCode addSymbol(final CoreSymbolSpecification symbolSpecification) {

        //log.debug("symbolSpecification: {}", symbolSpecification);

        final int symbolId = symbolSpecification.symbolId;
        if (orderBooks.get(symbolId) != null) {
            return CommandResultCode.MATCHING_ORDER_BOOK_ALREADY_EXISTS;
        } else {
            orderBooks.put(symbolId, orderBookFactory.apply(symbolSpecification));
            return CommandResultCode.SUCCESS;
        }
    }

    private void processMatchingCommand(final OrderCommand cmd) {

        final IOrderBook orderBook = orderBooks.get(cmd.symbol);
        if (orderBook == null) {
            cmd.resultCode = CommandResultCode.MATCHING_INVALID_ORDER_BOOK_ID;
        } else {
            cmd.resultCode = IOrderBook.processCommand(orderBook, cmd);

            // posting market data for risk processor makes sense only if command execution is successful, otherwise it will be ignored (possible garbage from previous cycle)
            // TODO don't need for EXCHANGE mode order books?
            // TODO doing this for many order books simultaneously can introduce hiccups
            if ((cmd.serviceFlags & 1) != 0 && cmd.command != ORDER_BOOK_REQUEST && cmd.resultCode == CommandResultCode.SUCCESS) {
                cmd.marketData = orderBook.getL2MarketDataSnapshot(8);
            }
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(shardId).writeLong(shardMask);
        binaryCommandsProcessor.writeMarshallable(bytes);

        // write orderBooks
        Utils.marshallIntHashMap(orderBooks, bytes);
    }

    @Override
    public int stateHash() {
        return Objects.hash(
                shardId,
                shardMask,
                binaryCommandsProcessor.stateHash(),
                Utils.stateHash(orderBooks));

        //log.debug("HASH ME{} : hash={} a={} b={}", shardId, hash, a, b);
    }
}
