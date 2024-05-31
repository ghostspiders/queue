package org.queue.server;

/**
 * @author gaoyvfeng
 * @ClassName QueueRequestHandlers
 * @description:
 * @datetime 2024年 05月 23日 16:08
 * @version: 1.0
 */
import org.queue.api.*;
import org.queue.common.ErrorMapping;
import org.queue.log.Log;
import org.queue.log.LogManager;
import org.queue.message.MessageSet;
import org.queue.network.Handler;
import org.queue.network.Receive;
import org.queue.network.Send;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Level;

public class QueueRequestHandlers {

    private LogManager logManager;
    private static final Logger logger = LoggerFactory.getLogger(QueueRequestHandlers.class.getName());
    private static final Logger requestLogger = LoggerFactory.getLogger("org.queue.request.logger");

    public QueueRequestHandlers(LogManager logManager) {
        this.logManager = logManager;
    }

    public Handler.HandlerType handlerFor(short requestTypeId, Receive receive) {
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

    private Optional<Send> handleProducerRequest(Receive receive) throws IOException, InterruptedException {
        long sTime = System.currentTimeMillis();
        ProducerRequest request = ProducerRequest.readFrom(receive.getBuffer());

        if (requestLogger.isDebugEnabled()) {
            requestLogger.debug("Producer request " + request.toString());
        }
        handleProducerRequest(request, "ProduceRequest");
        if (logger.isDebugEnabled()) {
            logger.debug("queue produce time " + (System.currentTimeMillis() - sTime) + " ms");
        }
        return Optional.empty();
    }

    private Optional<Send> handleMultiProducerRequest(Receive receive) {
        MultiProducerRequest request = MultiProducerRequest.readFrom(receive.getBuffer());
        if (requestLogger.isDebugEnabled()) {
            requestLogger.debug("Multiproducer request ");
        }
        request.getProduces().forEach(r -> {
            try {
                handleProducerRequest(r, "MultiProducerRequest");
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        return Optional.empty();
    }

    private void handleProducerRequest(ProducerRequest request, String requestHandlerName) throws IOException, InterruptedException {
        int partition = request.getTranslatedPartition(String.valueOf(logManager.chooseRandomPartition(null)));
        try {
            Log log = logManager.getOrCreateLog(request.getTopic(), partition);
            log.append(request.getMessages());
            if (logger.isDebugEnabled()) {
                logger.debug(request.getMessages().sizeInBytes() + " bytes written to logs.");
            }
        } catch (Throwable e) {
            logger.error("error processing " + requestHandlerName + " on " + request.getTopic() + ":" + partition);
            if (e instanceof IOException) {
                logger.error("force shutdown due to " + e);
                Runtime.getRuntime().halt(1);
            }
            throw e;
        }
    }

    private Optional<Send> handleFetchRequest(Receive request) {
        FetchRequest fetchRequest = FetchRequest.readFrom(request.getBuffer());
        if (requestLogger.isDebugEnabled()) {
            requestLogger.debug("Fetch request " + fetchRequest.toString());
        }
        return Optional.of(readMessageSet(fetchRequest));
    }

    private Optional<Send> handleMultiFetchRequest(Receive receive) {
        MultiFetchRequest multiFetchRequest = MultiFetchRequest.readFrom(receive.getBuffer());
        if (requestLogger.isDebugEnabled()) {
            requestLogger.debug("Multifetch request");
        }
        List<FetchRequest> fetches = multiFetchRequest.getFetches();
        fetches.forEach(req -> requestLogger.error(req.toString()));
        List<MessageSetSend> responses = new ArrayList<>();
        fetches.forEach(fetch -> responses.add(readMessageSet(fetch)));

        return Optional.of(new MultiMessageSetSend(responses));
    }

    private MessageSetSend readMessageSet(FetchRequest fetchRequest) {
        MessageSetSend response = null;
        try {
            logger.info("Fetching log segment for topic = " + fetchRequest.getTopic() + " and partition = " + fetchRequest.getPartition());
            Log log = logManager.getOrCreateLog(fetchRequest.getTopic(), fetchRequest.getPartition());
            response = new MessageSetSend(log.read(fetchRequest.getOffset(), fetchRequest.getMaxSize()));
        } catch (Throwable e) {
            logger.error("error when processing request " + fetchRequest);
            response = new MessageSetSend(MessageSet.Empty, ErrorMapping.codeFor(e.getClass()));
        }
        return response;
    }

    private Optional<Send> handleOffsetRequest(Receive request) throws IOException, InterruptedException {
        OffsetRequest offsetRequest = OffsetRequest.readFrom(request.getBuffer());
        if (requestLogger.isDebugEnabled()) {
            requestLogger.debug("Offset request " + offsetRequest.toString());
        }
        Log log = logManager.getOrCreateLog(offsetRequest.getTopic(), offsetRequest.getPartition());
        long[] offsets = log.getOffsetsBefore(offsetRequest);
        OffsetArraySend response = new OffsetArraySend(offsets);
        return Optional.of(response);
    }
}
