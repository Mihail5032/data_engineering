package ru.x5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.OutputTag;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import ru.x5.config.PropertiesHolder;
import ru.x5.factory.KafkaSourceFactory;
import ru.x5.factory.StreamExecutionEnvironmentFactory;
import ru.x5.process.RawDataProcessFunction;
import ru.x5.process.StreamSideOutputTag;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataStreamJob {
    public static final String CATALOG = "raw_table";
    public static final String SCHEMA = "raw_table";
    private final Map<String, TableIdentifier> tableMap = new HashMap<>();


    static {
        PropertiesHolder props = PropertiesHolder.getInstance();
        System.setProperty("aws.accessKeyId", props.getAccessKey());
        System.setProperty("aws.secretAccessKey", props.getSecretKey());
        System.setProperty("aws.region", "us-east-1");
    }

    public static void main(String[] args) throws Exception {
        DataStreamJob dataStreamJob = new DataStreamJob();
        dataStreamJob.run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironmentFactory.getStreamExecutionEnvironment();
        DataStreamSource<String> stream = env.fromSource(
                KafkaSourceFactory.buildKafkaSource(),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");

        PropertiesHolder config = PropertiesHolder.getInstance();
        CatalogLoader catalogLoader = CatalogLoader.hive(CATALOG, config.getEntriesHMS(), config.getEntriesS3());

        Map<String, TableLoader> tableLoaders = new HashMap<>();
        Map<String, Schema> icebergSchemas = new HashMap<>();

        tableMap.put("E1BPSOURCEDOCUMENTLI", TableIdentifier.of(SCHEMA, "raw_bpsourcedocumentli"));
        tableMap.put("E1BPFINACIALMOVEMENT", TableIdentifier.of(SCHEMA, "raw_bpfinacialmovement"));
        tableMap.put("E1BPFINANCIALMOVEMEN", TableIdentifier.of(SCHEMA, "raw_bpfinancialmovemen"));
        tableMap.put("E1BPLINEITEMDISCEXT", TableIdentifier.of(SCHEMA, "raw_bplineitemdiscext"));
        tableMap.put("E1BPLINEITEMDISCOUNT", TableIdentifier.of(SCHEMA, "raw_bplineitemdiscount"));
//        tableMap.put("E1BPLINEITEMEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bplineitemextensio"));
        tableMap.put("E1BPLINEITEMEXTENSIO", TableIdentifier.of(SCHEMA, "bplineitemextensio_pre"));
        tableMap.put("E1BPLINEITEMTAX", TableIdentifier.of(SCHEMA, "raw_bplineitemtax"));
        tableMap.put("E1BPRETAILLINEITEM", TableIdentifier.of(SCHEMA, "raw_bpretaillineitem"));
        tableMap.put("E1BPRETAILTOTALS", TableIdentifier.of(SCHEMA, "raw_bpretailtotals"));
        tableMap.put("E1BPTENDER", TableIdentifier.of(SCHEMA, "raw_bptender"));
//        tableMap.put("E1BPTENDER", TableIdentifier.of(SCHEMA, "bptender_pre"));
        tableMap.put("E1BPTENDEREXTENSIONS", TableIdentifier.of(SCHEMA, "raw_bptenderextensions"));
//        tableMap.put("E1BPTENDEREXTENSIONS", TableIdentifier.of(SCHEMA, "bptenderextensions_pre"));
        tableMap.put("E1BPTENDERTOTALS", TableIdentifier.of(SCHEMA, "raw_bptendertotals"));
        tableMap.put("E1BPTRANSACTDISCEXT", TableIdentifier.of(SCHEMA, "raw_bptransactdiscext"));
        tableMap.put("E1BPTRANSACTION", TableIdentifier.of(SCHEMA, "raw_bptransaction"));
//        tableMap.put("E1BPTRANSACTION", TableIdentifier.of(SCHEMA, "bptransaction_pre"));
        tableMap.put("E1BPTRANSACTIONDISCO", TableIdentifier.of(SCHEMA, "raw_bptransactiondisco"));
        tableMap.put("E1BPTRANSACTEXTENSIO", TableIdentifier.of(SCHEMA, "raw_bptransactextensio"));
//        tableMap.put("E1BPTRANSACTEXTENSIO", TableIdentifier.of(SCHEMA, "bptransactextensio_pre"));
        tableMap.put("E1BPLINEITEMVOID", TableIdentifier.of(SCHEMA, "raw_bplineitemvoid"));
        tableMap.put("E1BPPOSTVOIDDETAILS", TableIdentifier.of(SCHEMA, "raw_bppostvoiddetails"));

        for (Map.Entry<String, TableIdentifier> entry : tableMap.entrySet()) {
            String segment = entry.getKey();
            TableIdentifier tableId = entry.getValue();
            TableLoader loader = TableLoader.fromCatalog(catalogLoader, tableId);
            tableLoaders.put(segment, loader);
            Schema schema = getIcebergSchema(loader);
            icebergSchemas.put(segment, schema);
        }

        SingleOutputStreamOperator<RowData> rowData = stream.process(new RawDataProcessFunction(icebergSchemas));

        for (Map.Entry<String, TableIdentifier> entry : tableMap.entrySet()) {
            String segment = entry.getKey();
            OutputTag<RowData> tag = StreamSideOutputTag.getTag(segment);
            TableLoader loader = tableLoaders.get(segment);

            DataStream<RowData> sideStream = rowData.getSideOutput(tag);

            FlinkSink.forRowData(sideStream)
                    .tableLoader(loader)
                    .writeParallelism(1)
                    .upsert(false)
                    .set("write.target-file-size-bytes", "268435456")
                    .append();
        }

        env.execute("XML Parser with correction 11");
        closeTableLoaders(tableLoaders);
        env.close();
    }

    private Schema getIcebergSchema(TableLoader tableLoaderDynamic) throws IOException {
        Table icebergTableDynamic;
        try (tableLoaderDynamic) {
            tableLoaderDynamic.open();
            icebergTableDynamic = tableLoaderDynamic.loadTable();
        }
        return icebergTableDynamic.schema();
    }

    private void closeTableLoaders(Map<String, TableLoader> tableLoaders) {
        tableLoaders.values().forEach(loader -> {
            try {
                loader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
