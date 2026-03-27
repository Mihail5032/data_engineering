package ru.x5.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class TenderPstProcessor {
    private List<BaseTransactionKey> data;

    public List<Tender> prepareTenderPst() {
        List<Transaction> tx = data.stream()
                .filter(x -> x.getSegmentName().contains("E1BPTRANSACTION"))
                .map(x -> (Transaction) x)
                .collect(Collectors.toList());
        List<Tender> te = data.stream()
                .filter(x -> x.getSegmentName().contains("E1BPTENDER"))
                .map(x -> (Tender) x)
                .collect(Collectors.toList());
        List<TenderExtension> tex = data.stream()
                .filter(x -> x.getSegmentName().contains("E1BPTENDEREXTENSIONS"))
                .map(x -> (TenderExtension) x)
                .collect(Collectors.toList());

        BigDecimal sumSalesAmount = data.stream()
                .filter(x -> x.getSegmentName().contains("E1BPRETAILLINEITEM"))
                .map(x -> ((RetailLineItem) x).getSalesAmount())
                .reduce(BigDecimal::add)
                .orElse(BigDecimal.ZERO);
        BigDecimal sumTenderAmount = te.stream()
                .map(Tender::getTenderAmount)
                .reduce(BigDecimal::add)
                .orElse(BigDecimal.ZERO);

        boolean isVprokExpress = data.stream()
                .filter(x -> x.getSegmentName().contains("E1BPTRANSACTEXTENSIO"))
                .map(x -> (TransactionExtension) x)
                .filter(x -> x.getFieldName().contains("SOURCE"))
                .anyMatch(x -> x.getFieldValue().contains("vprok.express"));

        List<Tender> tenders = prepareFirstPart(tx, sumSalesAmount, sumTenderAmount, isVprokExpress);
        tenders.addAll(prepareSecondPart(tx, sumSalesAmount, isVprokExpress));
        // мутация 3108→3123 уже делается в Tender.toRowDataPst(),
        // prepareThirdPart возвращала ВСЕ оригинальные тендеры, что приводило к дублям 3101
        applyThirdPartMutation(te, tex);
        return tenders;
    }

    private List<Tender> prepareFirstPart(List<Transaction> tx,
                                           BigDecimal sumSalesAmount,
                                           BigDecimal sumTenderAmount,
                                           boolean isVprokExpress) {
        BigDecimal tenderAmount = sumSalesAmount.subtract(sumTenderAmount);
        if (tenderAmount.abs().compareTo(BigDecimal.ONE) < 1) {
            return new ArrayList<>();
        }
        List<Transaction> tx1014 = tx.stream()
                .filter(x -> x.getTransactionTypeCode().equals("1014"))
                .collect(Collectors.toList());
        if (tx1014.isEmpty()) {
            return new ArrayList<>();
        }
        return tx1014.stream()
                .map(x -> {
                    Tender tender = new Tender(
                            x.transactionSequenceNumber,
                            "3101",
                            tenderAmount,
                            null,
                            null,
                            null,
                            null,
                            null
                    );
                    tender.setWorkstationId(isVprokExpress ? "0000001002" : x.workstationId);
                    tender.setRetailStoreId(x.retailStoreId);
                    tender.setBusinessDayDate(x.businessDayDate);
                    tender.setTransactionTypeCode(x.transactionTypeCode);
                    return tender;
                })
                .collect(Collectors.toList());
    }

    private List<Tender> prepareSecondPart(List<Transaction> tx,
                                          BigDecimal sumSalesAmount,
                                          boolean isVprokExpress) {
        List<Transaction> tx1014 = tx.stream()
                .filter(x -> x.getTransactionTypeCode().equals("1014"))
                .collect(Collectors.toList());
        if (tx1014.isEmpty()) {
            return new ArrayList<>();
        }
        return tx1014.stream()
                .map(x -> {
                    Tender tender = new Tender(
                            x.transactionSequenceNumber,
                            "3101",
                            sumSalesAmount,
                            null,
                            null,
                            null,
                            null,
                            null
                    );
                    tender.setWorkstationId(isVprokExpress ? "0000001002" : x.workstationId);
                    tender.setRetailStoreId(x.retailStoreId);
                    tender.setBusinessDayDate(x.businessDayDate);
                    tender.setTransactionTypeCode(x.transactionTypeCode);
                    return tender;
                })
                .collect(Collectors.toList());
    }

    /**
     * Мутирует оригинальные тендеры 3108→3123 при наличии CERT_PARTY=RU02.
     * Не возвращает список — мутация применяется напрямую к объектам.
     * Раньше prepareThirdPart возвращала все оригинальные тендеры,
     * что приводило к повторной записи тендеров с типом 3101.
     */
    private void applyThirdPartMutation(List<Tender> te, List<TenderExtension> tex) {
        boolean isCertParty = tex.stream()
                .filter(x -> x.getFieldName().contains("CERT_PARTY"))
                .anyMatch(x -> x.getFieldValue().contains("RU02"));
        if (isCertParty) {
            te.forEach(x -> {
                if ("3108".equals(x.getTenderTypeCode())) {
                    x.setTenderTypeCode("3123");
                }
            });
        }
    }

}
