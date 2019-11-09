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
package exchange.core2.core.processors;

import exchange.core2.core.common.CoreSymbolSpecification;
import exchange.core2.core.common.Order;
import exchange.core2.core.common.StateHash;
import exchange.core2.core.common.SymbolType;
import exchange.core2.core.common.api.binary.BatchAddAccountsCommand;
import exchange.core2.core.common.api.binary.BatchAddSymbolsCommand;
import exchange.core2.core.common.api.reports.*;
import exchange.core2.core.common.cmd.CommandResultCode;
import exchange.core2.core.common.cmd.OrderCommand;
import exchange.core2.core.common.cmd.OrderCommandType;
import exchange.core2.core.orderbook.IOrderBook;
import exchange.core2.core.processors.journalling.ISerializationProcessor;
import exchange.core2.core.utils.CoreArithmeticUtils;
import exchange.core2.core.utils.HashingUtils;
import exchange.core2.core.utils.SerializationUtils;
import exchange.core2.core.utils.UnsafeUtils;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.impl.map.mutable.primitive.IntLongHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

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
                        final IntObjectHashMap<IOrderBook> ob = SerializationUtils.readIntHashMap(bytesIn, IOrderBook::create);
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

        if (command == OrderCommandType.MOVE_ORDER || command == OrderCommandType.CANCEL_ORDER || command == OrderCommandType.ORDER_BOOK_REQUEST || command == OrderCommandType.PLACE_ORDER) {
            // process specific symbol group only
            if (symbolForThisHandler(cmd.symbol)) {
                processMatchingCommand(cmd);
            }
        } else if (command == OrderCommandType.BINARY_DATA) {

            final boolean isLastFrame = binaryCommandsProcessor.acceptBinaryFrame(cmd);
            if (shardId == 0) {
                cmd.resultCode = isLastFrame ? CommandResultCode.SUCCESS : CommandResultCode.ACCEPTED;
            }

        } else if (command == OrderCommandType.RESET) {
            // process all symbols groups, only processor 0 writes result
            orderBooks.clear();
            binaryCommandsProcessor.reset();
            if (shardId == 0) {
                cmd.resultCode = CommandResultCode.SUCCESS;
            }

        } else if (command == OrderCommandType.PERSIST_STATE_MATCHING) {
            final boolean isSuccess = serializationProcessor.storeData(cmd.orderId, ISerializationProcessor.SerializedModuleType.MATCHING_ENGINE_ROUTER, shardId, this);
            // Send ACCEPTED because this is a first command in series. Risk engine is second - so it will return SUCCESS
            UnsafeUtils.setResultVolatile(cmd, isSuccess, CommandResultCode.ACCEPTED, CommandResultCode.STATE_PERSIST_MATCHING_ENGINE_FAILED);
        }

    }


    private Optional<? extends WriteBytesMarshallable> handleBinaryMessage(Object message) {
        if (message instanceof BatchAddSymbolsCommand) {
            // TODO return status object
            final IntObjectHashMap<CoreSymbolSpecification> symbols = ((BatchAddSymbolsCommand) message).getSymbols();
            symbols.forEach(this::addSymbol);
            return Optional.empty();
        } else if (message instanceof BatchAddAccountsCommand) {
            // do nothing
            return Optional.empty();
        } else if (message instanceof ReportQuery) {
            return processReport((ReportQuery) message);
        } else {
            return Optional.empty();
        }
    }


    private Optional<? extends WriteBytesMarshallable> processReport(ReportQuery reportQuery) {

        switch (reportQuery.getReportType()) {

            case STATE_HASH:
                return reportStateHash();

            case SINGLE_USER_REPORT:
                return reportSingleUser((SingleUserReportQuery) reportQuery);


            case TOTAL_CURRENCY_BALANCE:
                return reportGlobalBalance();

            default:
                throw new IllegalStateException("Report not implemented");
        }
    }

    private Optional<StateHashReportResult> reportStateHash() {
        return Optional.of(new StateHashReportResult(stateHash()));
    }

    private Optional<SingleUserReportResult> reportSingleUser(final SingleUserReportQuery query) {
        final IntObjectHashMap<List<Order>> orders = new IntObjectHashMap<>();
        orderBooks.forEach(ob -> orders.put(ob.getSymbolSpec().symbolId, ob.findUserOrders(query.getUid())));

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
                            ob.askOrdersStream(false).mapToLong(ord -> CoreArithmeticUtils.calculateAmountAsk(ord.size - ord.filled, spec)).sum());

                    currencyBalance.addToValue(
                            spec.getQuoteCurrency(),
                            ob.bidOrdersStream(false).mapToLong(ord -> CoreArithmeticUtils.calculateAmountBidTakerFee(ord.size - ord.filled, ord.reserveBidPrice, spec)).sum());
                });

        return Optional.of(TotalCurrencyBalanceReportResult.ofOrderBalances(currencyBalance));
    }


    private boolean symbolForThisHandler(final long symbol) {
        return (shardMask == 0) || ((symbol & shardMask) == shardId);
    }


    private CommandResultCode addSymbol(final CoreSymbolSpecification symbolSpecification) {

//        log.debug("symbolSpecification: {}", symbolSpecification);

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
            if ((cmd.serviceFlags & 1) != 0 && cmd.command != OrderCommandType.ORDER_BOOK_REQUEST && cmd.resultCode == CommandResultCode.SUCCESS) {
                cmd.marketData = orderBook.getL2MarketDataSnapshot(8);
            }
        }
    }

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(shardId).writeLong(shardMask);
        binaryCommandsProcessor.writeMarshallable(bytes);

        // write orderBooks
        SerializationUtils.marshallIntHashMap(orderBooks, bytes);
    }

    @Override
    public int stateHash() {
        return Objects.hash(
                shardId,
                shardMask,
                binaryCommandsProcessor.stateHash(),
                HashingUtils.stateHash(orderBooks));

        //log.debug("HASH ME{} : hash={} a={} b={}", shardId, hash, a, b);
    }
}
