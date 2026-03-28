package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class PostrCreateMultip {
    @XmlElement(name = "I_COMMIT")
    private String iCommit;

    @XmlElement(name = "I_LOCKWAIT")
    private String iLockwait;

    @XmlElement(name = "_-POSDW_-E1BPSOURCEDOCUMENTLI")
    private SourceDocument sourceDocument;

    @XmlElement(name = "_-POSDW_-E1BPTRANSACTION")
    private List<Transaction> transactions = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPRETAILLINEITEM")
    private List<RetailLineItem> retailLineItems = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPLINEITEMTAX")
    private List<LineItemTax> lineItemTaxes = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPTRANSACTIONDISCO")
    private List<TransactionDiscount> transactionDiscounts = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPLINEITEMDISCOUNT")
    private List<LineItemDiscount> lineItemDiscounts = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPLINEITEMDISCEXT")
    private List<LineItemDiscExt> lineItemDiscExtensions = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPLINEITEMEXTENSIO")
    private List<LineItemExtension> lineItemExtensions = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPFINANCIALMOVEMEN")
    private List<FinancialMovemen> financialMovemen = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPFINACIALMOVEMENT")
    private List<FinacialMovement> finacialMovement = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPRETAILTOTALS")
    private List<RetailTotals> retailTotals = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPTENDERTOTALS")
    private List<TenderTotals> tenderTotals = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPTENDER")
    private List<Tender> tenders = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPTRANSACTEXTENSIO")
    private List<TransactionExtension> transactionExtensions = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPTENDEREXTENSIONS")
    private List<TenderExtension> tenderExtensions = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPTRANSACTDISCEXT")
    private List<TransactDiscExt> transactDiscExts = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPLINEITEMVOID")
    private List<LineItemVoid> lineItemVoids = new ArrayList<>();

    @XmlElement(name = "_-POSDW_-E1BPPOSTVOIDDETAILS")
    private List<PostVoidDetails> postVoidDetails = new ArrayList<>();

}
