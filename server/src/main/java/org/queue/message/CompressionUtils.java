package org.queue.message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressionUtils {

    private static final Logger logger = LoggerFactory.getLogger(CompressionUtils.class);

    public static Message compress(Iterable<Message> messages) {
        return compress(messages, new DefaultCompressionCodec());
    }

    public static Message compress(Iterable<Message> messages, CompressionCodec compressionCodec) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GZIPOutputStream gzipOutput = null;
        try {
            gzipOutput = new GZIPOutputStream(outputStream);
            if (logger.isDebugEnabled()) {
                logger.debug("Allocating message byte buffer of size = " + MessageSet.messageSetSize(messages));
            }

            ByteBuffer messageByteBuffer = ByteBuffer.allocate(MessageSet.messageSetSize(messages));
            for (Message m : messages) {
                m.serializeTo(messageByteBuffer);
            }
            messageByteBuffer.rewind();

            gzipOutput.write(messageByteBuffer.array());
            gzipOutput.finish();

            Message compressedMessage = new Message(outputStream.toByteArray(), compressionCodec);
            return compressedMessage;
        } catch (IOException e) {
            logger.error("Error while writing to the GZIP output stream: " + e.getMessage());
            try {
                if (gzipOutput != null) gzipOutput.close();
                if (outputStream != null) outputStream.close();
            } catch (IOException ex) {
                // Ignored, already logging the original exception
            }
            throw new RuntimeException(e);
        } finally {
            try {
                if (gzipOutput != null) gzipOutput.close();
                if (outputStream != null) outputStream.close();
            } catch (IOException ex) {
                // Ignored, already logging the original exception
            }
        }
    }

    public static ByteBufferMessageSet decompress(Message message) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        InputStream inputStream = new ByteBufferBackedInputStream(message.payload());
        GZIPInputStream gzipIn = null;
        try {
            gzipIn = new GZIPInputStream(inputStream);
            byte[] intermediateBuffer = new byte[1024];

            int dataRead;
            while ((dataRead = gzipIn.read(intermediateBuffer)) != -1) {
                outputStream.write(intermediateBuffer, 0, dataRead);
            }

            ByteBuffer outputBuffer = ByteBuffer.allocate(outputStream.size());
            outputBuffer.put(outputStream.toByteArray());
            outputBuffer.rewind();
            return new ByteBufferMessageSet(outputBuffer);
        } catch (IOException e) {
            logger.error("Error while reading from the GZIP input stream: " + e.getMessage());
            try {
                if (gzipIn != null) gzipIn.close();
                if (outputStream != null) outputStream.close();
            } catch (IOException ex) {
                // Ignored, already logging the original exception
            }
            throw new RuntimeException(e);
        } finally {
            try {
                if (gzipIn != null) gzipIn.close();
                if (outputStream != null) outputStream.close();
            } catch (IOException ex) {
                // Ignored, already logging the original exception
            }
        }
    }
}
