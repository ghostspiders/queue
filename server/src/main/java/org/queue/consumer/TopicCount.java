package org.queue.consumer;

/**
 * @author gaoyvfeng
 * @ClassName TopicCount
 * @description:
 * @datetime 2024年 05月 24日 10:01
 * @version: 1.0
 */
import com.google.gson.Gson;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopicCount {
    private static final Logger logger = LoggerFactory.getLogger(TopicCount.class);
    private final String consumerIdString;
    private final Map<String, Integer> topicCountMap;
    private static Gson gson = new Gson();

    public static TopicCount constructTopicCount(String consumerIdString, String jsonString) {
        if ("{  }".equals(jsonString)) {
            return new TopicCount(consumerIdString, gson.fromJson("", Map.class));
        }

        Map<String, Integer> value;
        try {
            value = gson.fromJson(jsonString, Map.class);
            if (value == null) {
                throw new RuntimeException("error constructing TopicCount: " + jsonString);
            }
        } catch (Throwable e) {
            logger.error("error parsing consumer json string " + jsonString, e);
            throw e;
        }
        // 待办：处理JSON转换问题
        return new TopicCount(consumerIdString, new HashMap<String, Integer>() {{ put("topic1", 1); }});
    }


    public TopicCount(String consumerIdString, Map<String, Integer> topicCountMap) {
        this.consumerIdString = consumerIdString;
        this.topicCountMap = topicCountMap;
    }

    public Map<String, Set<String>> getConsumerThreadIdsPerTopic() {
        Map<String, Set<String>> consumerThreadIdsPerTopicMap = new HashMap<>();
        for (Map.Entry<String, Integer> entry : topicCountMap.entrySet()) {
            Set<String> consumerSet = new HashSet<>();
            assert (entry.getValue() >= 1);
            for (int i = 0; i < entry.getValue(); i++) {
                consumerSet.add(consumerIdString + "-" + i);
            }
            consumerThreadIdsPerTopicMap.put(entry.getKey(), consumerSet);
        }
        return consumerThreadIdsPerTopicMap;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TopicCount topicCount = (TopicCount) obj;
        return consumerIdString.equals(topicCount.consumerIdString) &&
                topicCountMap.equals(topicCount.topicCountMap);
    }

    /**
     * 返回形如
     * { "topic1" : 4,
     *   "topic2" : 4
     * }
     * 的JSON字符串。
     */
    public String toJsonString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{ ");
        int i = 0;
        for (Map.Entry<String, Integer> entry : topicCountMap.entrySet()) {
            if (i++ > 0) builder.append(", ");
            builder.append("\"").append(entry.getKey()).append("\": ").append(entry.getValue());
        }
        builder.append(" }");
        return builder.toString();
    }
}
