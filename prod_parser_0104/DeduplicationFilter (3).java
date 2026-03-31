package ru.x5.process;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import java.time.Duration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Фильтр дедупликации транзакций на основе rtl_txn_rk (4 поля:
 * retailStoreId, businessDayDate, workstationId, transactionSequenceNumber).
 *
 * Хранит state глубиной 72 часа. Если транзакция с таким ключом
 * уже была обработана — молча отбрасывает дубль.
 *
 * Размещается в пайплайне ПЕРЕД RawDataProcessFunction:
 *   Kafka -> keyBy(DeduplicationKeyExtractor) -> DeduplicationFilter -> RawDataProcessFunction -> sinks
 */
public class DeduplicationFilter extends KeyedProcessFunction<String, String, String> {

    private static final Logger log = LogManager.getLogger("ru.x5.parser");

    private static final long TTL_HOURS = 72L;

    // Счётчики — логируем итог каждые LOG_INTERVAL сообщений
    private static final long LOG_INTERVAL = 100_000L;
    private transient long totalCount = 0;
    private transient long duplicateCount = 0;

    /**
     * State: true если транзакция с данным ключом уже была обработана.
     * Flink автоматически удаляет запись через 72 часа (TTL).
     */
    private transient ValueState<Boolean> seenState;

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(TTL_HOURS))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("seen-rtl-txn-rk", Boolean.class);
        descriptor.enableTimeToLive(ttlConfig);

        seenState = getRuntimeContext().getState(descriptor);

        log.info("DEDUP_INIT: DeduplicationFilter started, TTL={}h, subtask={}",
                TTL_HOURS, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    @Override
    public void processElement(String kafkaValue, Context ctx, Collector<String> out) throws Exception {
        totalCount++;
        String key = ctx.getCurrentKey();

        if (seenState.value() != null) {
            // Дубль — транзакция с таким ключом уже обработана, отбрасываем
            duplicateCount++;
            log.warn("DEDUP_DROP: key={}", key);
            return;
        }

        // Первый раз видим эту транзакцию — запоминаем и пропускаем дальше
        seenState.update(true);
        out.collect(kafkaValue);

        // Периодическая статистика — каждые 100К сообщений
        if (totalCount % LOG_INTERVAL == 0) {
            log.info("DEDUP_STATS: total={}, duplicates={}, passed={}, subtask={}",
                    totalCount, duplicateCount, totalCount - duplicateCount,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
    }
}
