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

            StringBuilder sb = new StringBuilder();
            char[] buffer = new char[8192];
            int charsRead;
            while ((charsRead = reader.read(buffer)) != -1) {
                sb.append(buffer, 0, charsRead);
            }
            String xml = sb.toString();

            String retailStoreId = extractFirst(RETAIL_STORE_ID, xml);
            String businessDayDate = extractFirst(BUSINESS_DAY_DATE, xml);
            String workstationId = extractFirst(WORKSTATION_ID, xml);
            String txnSeqNum = extractFirst(TXN_SEQ_NUM, xml);

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
