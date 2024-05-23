package org.queue.server;

/**
 * @author gaoyvfeng
 * @ClassName KafkaRequestHandlers
 * @description:
 * @datetime 2024年 05月 23日 16:08
 * @version: 1.0
 */
import org.queue.api.MultiProducerRequest;
import org.queue.api.ProducerRequest;
import org.queue.api.RequestKeys;
import org.queue.log.LogManager;
import org.queue.network.Handler;
import org.queue.network.Receive;
import org.queue.network.Send;

import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

// 假设存在一个LogManager类，以及ProducerRequest等请求类和Send响应类
public class QueueRequestHandlers {
    private LogManager logManager;
    private static final Logger logger = Logger.getLogger(QueueRequestHandlers.class.getName());
    private static final Logger requestLogger = Logger.getLogger("org.queue.request.logger");

    public QueueRequestHandlers(LogManager logManager) {
        this.logManager = logManager;
    }

    public Handler handlerFor(short requestTypeId, Receive request) {
        switch (requestTypeId) {
            case RequestKeys.produce:
                return this::handleProducerRequest;
            case RequestKeys.fetch:
                return this::handleFetchRequest;
            case RequestKeys.multiFetch:
                return this::handleMultiFetchRequest;
            case RequestKeys.multiProduce:
                return this::handleMultiProducerRequest;
            case RequestKeys.offsets:
                return this::handleOffsetRequest;
            default:
                throw new IllegalStateException("No mapping found for handler id " + requestTypeId);
        }
    }

    private Optional<Send> handleProducerRequest(Receive receive) {
        long sTime = System.currentTimeMillis();
        ProducerRequest request = ProducerRequest.readFrom(receive.getBuffer());

        if (requestLogger.isLoggable(Level.FINER)) {
            requestLogger.fine("Producer request " + request.toString());
        }
        handleProducerRequest(request, "ProduceRequest");
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("queue produce time " + (System.currentTimeMillis() - sTime) + " ms");
        }
        return Optional.empty();
    }

    private Optional<Send> handleMultiProducerRequest(Receive receive) {
        MultiProducerRequest request = MultiProducerRequest.readFrom(receive.getBuffer());
        if (requestLogger.isLoggable(Level.FINER)) {
            requestLogger.fine("Multiproducer request ");
        }
        request.getProduces().forEach(r -> handleProducerRequest(r, "MultiProducerRequest"));
        return Optional.empty();
    }

    private void handleProducerRequest(ProducerRequest request, String requestHandlerName) {
        int partition = request.getTranslatedPartition(logManager.chooseRandomPartition());
        try {
            LogManager.Log log = logManager.getOrCreateLog(request.getTopic(), partition);
            log.append(request.getMessages());
            if (logger.isLoggable(Level.FINER)) {
                logger.finer(request.getMessages().sizeInBytes() + " bytes written to logs.");
            }
        } catch (Throwable e) {
            logger.severe("error processing " + requestHandlerName + " on " + request.getTopic() + ":" + partition);
            if (e instanceof IOException) {
                logger.severe("force shutdown due to " + e);
                Runtime.getRuntime().halt(1);
            }
            throw e;
        }
    }

    private Optional<Send> handleFetchRequest(Receive request) {
        FetchRequest fetchRequest = FetchRequest.readFrom(request.getBuffer());
        if (requestLogger.isLoggable(Level.FINER)) {
            requestLogger.fine("Fetch request " + fetchRequest.toString());
        }
        return Optional.of(readMessageSet(fetchRequest));
    }

    private Optional<Send> handleMultiFetchRequest(Receive receive) {
        MultiFetchRequest multiFetchRequest = MultiFetchRequest.readFrom(receive.getBuffer());
        if (requestLogger.isLoggable(Level.FINER)) {
            requestLogger.fine("Multifetch request");
        }
        List<FetchRequest> fetches = multiFetchRequest.getFetches();
        fetches.forEach(req -> requestLogger.fine(req.toString()));
        List<MessageSetSend> responses = new ArrayList<>();
        fetches.forEach(fetch -> responses.add(readMessageSet(fetch)));

        return Optional.of(new MultiMessageSetSend(responses));
    }

    private MessageSetSend readMessageSet(FetchRequest fetchRequest) {
        // 假设存在MessageSetSend和LogManager.Log类
        MessageSetSend response = null;
        try {
            logger.fine("Fetching log segment for topic = " + fetchRequest.getTopic() + " and partition = " + fetchRequest.getPartition());
            LogManager.Log log = logManager.getOrCreateLog(fetchRequest.getTopic(), fetchRequest.getPartition());
            response = new MessageSetSend(log.read(fetchRequest.getOffset(), fetchRequest.getMaxSize()));
        } catch (Throwable e) {
            logger.severe("error when processing request " + fetchRequest);
            response = new MessageSetSend(MessageSet.Empty, ErrorMapping.codeFor(e.getClass()));
        }
        return response;
    }

    private Optional<Send> handleOffsetRequest(Receive request) {
        OffsetRequest offsetRequest = OffsetRequest.readFrom(request.getBuffer());
        if (requestLogger.isLoggable(Level.FINER)) {
            requestLogger.fine("Offset request " + offsetRequest.toString());
        }
        LogManager.Log log = logManager.getOrCreateLog(offsetRequest.getTopic(), offsetRequest.getPartition());
        long[] offsets = log.getOffsetsBefore(offsetRequest);
        OffsetArraySend response = new OffsetArraySend(offsets);
        return Optional.of(response);
    }
}
