package ru.x5.factory;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.x5.config.PropertiesHolder;

/**
 * Factory to create StreamExecutionEnvironment according property (local or cluster mode)
 */
public class StreamExecutionEnvironmentFactory {

    public static StreamExecutionEnvironment getStreamExecutionEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(180_000);// 3 минуты
        return env;
    }
}
