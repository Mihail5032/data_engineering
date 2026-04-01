package ru.x5.process;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;

/**
 * Дедупликация PST-записей на уровне отдельной транзакции (rtl_txn_rk).
 *
 * Проблема: один IDOC (Kafka-сообщение) содержит десятки транзакций.
 * DeduplicationFilter дедуплицирует на уровне IDOC (по первой транзакции).
 * Но одна и та же транзакция (одинаковый rtl_txn_rk) может прийти
 * в РАЗНЫХ IDOC-ах — и DeduplicationFilter её не поймает,
 * т.к. у каждого IDOC свой ключ.
 *
 * Этот фильтр ставится ПОСЛЕ side output и ПЕРЕД Iceberg sink:
 *   RawDataProcessFunction → getSideOutput(PST_BPTRANSACTION)
 *     → keyBy(PstTransactionKeySelector) → PstDeduplicationFilter → FlinkSink
 *
 * Хранит keyed state с TTL 72 часа — если транзакция с таким rtl_txn_rk
 * уже была записана, дубль отбрасывается.
 */
public class PstDeduplicationFilter extends KeyedProcessFunction<String, RowData, RowData> {

    private static final Logger log = LogManager.getLogger("ru.x5.parser");

    private static final long TTL_HOURS = 72L;
    private static final long LOG_INTERVAL = 100_000L;

    private transient ValueState<Boolean> seenState;
    private transient long totalCount = 0;
    private transient long duplicateCount = 0;

    @Override
    public void open(org.apache.flink.api.common.functions.OpenContext openContext) {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Duration.ofHours(TTL_HOURS))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupFullSnapshot()
                .build();

        ValueStateDescriptor<Boolean> descriptor =
                new ValueStateDescriptor<>("pst-seen-rtl-txn-rk", Boolean.class);
        descriptor.enableTimeToLive(ttlConfig);

        seenState = getRuntimeContext().getState(descriptor);

        log.info("PST_DEDUP_INIT: PstDeduplicationFilter started, TTL={}h, subtask={}",
                TTL_HOURS, getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
    }

    @Override
    public void processElement(RowData row, Context ctx, Collector<RowData> out) throws Exception {
        totalCount++;
        String key = ctx.getCurrentKey();

        if (seenState.value() != null) {
            // Дубль — транзакция с таким rtl_txn_rk уже обработана
            duplicateCount++;
            // Логируем первые 10 дублей + каждый 1000-й, чтобы не засорять лог
            if (duplicateCount <= 10 || duplicateCount % 1000 == 0) {
                log.warn("PST_DEDUP_DROP: key={}, total_dupes={}, subtask={}",
                        key, duplicateCount,
                        getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
            }
            return;
        }

        // Первый раз видим эту транзакцию — пропускаем и запоминаем
        seenState.update(true);
        out.collect(row);

        // Периодическая статистика
        if (totalCount % LOG_INTERVAL == 0) {
            log.info("PST_DEDUP_STATS: total={}, duplicates={}, passed={}, subtask={}",
                    totalCount, duplicateCount, totalCount - duplicateCount,
                    getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        }
    }
}
