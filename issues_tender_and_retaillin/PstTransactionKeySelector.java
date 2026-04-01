package ru.x5.process;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Извлекает ключ дедупликации rtl_txn_rk из RowData на основе Iceberg-схемы.
 *
 * Ключ = retailStoreId | businessDayDate | workstationId | transactionSequenceNumber
 *
 * Позиции полей определяются динамически из Iceberg Schema.
 * Поддерживаемые имена полей (регистронезависимо):
 *   - retailstoreid / retail_store_id
 *   - businessdaydate / business_day_date
 *   - workstationid / workstation_id
 *   - transactionsequencenumber / transaction_sequence_number
 */
public class PstTransactionKeySelector implements KeySelector<RowData, String> {

    private final int retailStoreIdPos;
    private final int businessDayDatePos;
    private final int workstationIdPos;
    private final int txnSeqNumPos;

    public PstTransactionKeySelector(Schema schema) {
        List<Types.NestedField> columns = schema.columns();
        this.retailStoreIdPos = findFieldPosition(columns,
                "retailstoreid", "retail_store_id");
        this.businessDayDatePos = findFieldPosition(columns,
                "businessdaydate", "business_day_date");
        this.workstationIdPos = findFieldPosition(columns,
                "workstationid", "workstation_id");
        this.txnSeqNumPos = findFieldPosition(columns,
                "transactionsequencenumber", "transaction_sequence_number");
    }

    /**
     * Ищет поле по нескольким вариантам имени (регистронезависимо).
     * Возвращает ПОЗИЦИЮ (индекс) поля в списке колонок.
     */
    private static int findFieldPosition(List<Types.NestedField> columns, String... names) {
        for (int i = 0; i < columns.size(); i++) {
            String colName = columns.get(i).name().toLowerCase();
            for (String name : names) {
                if (colName.equals(name.toLowerCase())) {
                    return i;
                }
            }
        }
        String available = columns.stream()
                .map(Types.NestedField::name)
                .collect(Collectors.joining(", "));
        throw new IllegalArgumentException(
                "PST dedup key field not found. Tried: " + String.join(", ", names)
                        + ". Available columns: [" + available + "]");
    }

    @Override
    public String getKey(RowData row) throws Exception {
        String storeId = row.isNullAt(retailStoreIdPos) ? ""
                : row.getString(retailStoreIdPos).toString();
        String dayDate = row.isNullAt(businessDayDatePos) ? ""
                : String.valueOf(row.getInt(businessDayDatePos));
        String wsId = row.isNullAt(workstationIdPos) ? ""
                : row.getString(workstationIdPos).toString();
        String seqNum = row.isNullAt(txnSeqNumPos) ? ""
                : row.getString(txnSeqNumPos).toString();

        return storeId + "|" + dayDate + "|" + wsId + "|" + seqNum;
    }
}
