package ru.x5.process;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.log4j.Logger;
import ru.x5.decoder.CustomDecoder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Извлекает ключ дедупликации (retailStoreId|businessDayDate|workstationId|transactionSequenceNumber)
 * из сырого Kafka-сообщения.
 *
 * Использует regex вместо JAXB — в ~10 раз быстрее полного парсинга.
 * Достаточно найти первые вхождения 4 тегов в XML.
 */
public class DeduplicationKeyExtractor implements KeySelector<String, String> {

    private static final Logger log = Logger.getLogger(DeduplicationKeyExtractor.class);

    private static final Pattern RETAIL_STORE_ID = Pattern.compile("<RETAILSTOREID>([^<]*)</RETAILSTOREID>");
    private static final Pattern BUSINESS_DAY_DATE = Pattern.compile("<BUSINESSDAYDATE>([^<]*)</BUSINESSDAYDATE>");
    private static final Pattern WORKSTATION_ID = Pattern.compile("<WORKSTATIONID>([^<]*)</WORKSTATIONID>");
    private static final Pattern TXN_SEQ_NUM = Pattern.compile("<TRANSACTIONSEQUENCENUMBER>([^<]*)</TRANSACTIONSEQUENCENUMBER>");

    @Override
    public String getKey(String kafkaValue) throws Exception {
        try (InputStream inputStream = CustomDecoder.decodeAndDecompress(kafkaValue)) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));

            String retailStoreId = "";
            String businessDayDate = "";
            String workstationId = "";
            String txnSeqNum = "";
            int found = 0;

            String line;
            while ((line = reader.readLine()) != null && found < 4) {
                if (retailStoreId.isEmpty()) {
                    String val = extractFirst(RETAIL_STORE_ID, line);
                    if (!val.isEmpty()) { retailStoreId = val; found++; }
                }
                if (businessDayDate.isEmpty()) {
                    String val = extractFirst(BUSINESS_DAY_DATE, line);
                    if (!val.isEmpty()) { businessDayDate = val; found++; }
                }
                if (workstationId.isEmpty()) {
                    String val = extractFirst(WORKSTATION_ID, line);
                    if (!val.isEmpty()) { workstationId = val; found++; }
                }
                if (txnSeqNum.isEmpty()) {
                    String val = extractFirst(TXN_SEQ_NUM, line);
                    if (!val.isEmpty()) { txnSeqNum = val; found++; }
                }
            }

            return retailStoreId + "|" + businessDayDate + "|" + workstationId + "|" + txnSeqNum;

        } catch (Exception e) {
            log.error("Failed to extract deduplication key, using raw hashCode as fallback", e);
        }

        return "UNKNOWN_" + kafkaValue.hashCode();
    }

    private String extractFirst(Pattern pattern, String xml) {
        Matcher matcher = pattern.matcher(xml);
        return matcher.find() ? matcher.group(1) : "";
    }
}
