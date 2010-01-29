/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.voxtelecom.voxzal.util;

import java.sql.Date;

/**
 *
 * @author khaya
 */
public class VoxcontractItemBillingInfoAdaptee {

    private String voxcontractItemBillingInfoId;
    private Integer masterNumber;
    private Integer previousMasterNumber;
    private String salesmanId;
    private Double sellingPrice;
    private Double corePrice;
    private Double lobPrice;
    private Double setupFee;
    private Double commisionableValue;
    private Boolean isBillable;
    private Date installedDate;
    private Date soldDate;
    private Boolean isProrate;
    private Date billingStartDate;
    private Date cancelledDate;
    private Boolean isActive;
    private String voxcontractDebitOrderDay;
    private String profitCenter;
    private Date upgradeDate;
    private Boolean upgradeDowngrade;
    private String note;
    private String paymentType;
    private Boolean cancellation;
    private String cancellationReason;
    private String accountManager;
    private String clientRef;
    private String region;
    private String voxcontractId;
    private String voxcontractProductId;

    public VoxcontractItemBillingInfoAdaptee() {
    }

    public String getAccountManager() {
        return accountManager;
    }

    public void setAccountManager(String accountManager) {
        this.accountManager = accountManager;
    }

    public Date getBillingStartDate() {
        return billingStartDate;
    }

    public void setBillingStartDate(Date billingStartDate) {
        this.billingStartDate = billingStartDate;
    }

    public Boolean getCancellation() {
        return cancellation;
    }

    public void setCancellation(Boolean cancellation) {
        this.cancellation = cancellation;
    }

    public String getCancellationReason() {
        return cancellationReason;
    }

    public void setCancellationReason(String cancellationReason) {
        this.cancellationReason = cancellationReason;
    }

    public Date getCancelledDate() {
        return cancelledDate;
    }

    public void setCancelledDate(Date cancelledDate) {
        this.cancelledDate = cancelledDate;
    }

    public String getClientRef() {
        return clientRef;
    }

    public void setClientRef(String clientRef) {
        this.clientRef = clientRef;
    }

    public Double getCommisionableValue() {
        return commisionableValue;
    }

    public void setCommisionableValue(Double commisionableValue) {
        this.commisionableValue = commisionableValue;
    }

    public Double getCorePrice() {
        return corePrice;
    }

    public void setCorePrice(Double corePrice) {
        this.corePrice = corePrice;
    }

    public Date getInstalledDate() {
        return installedDate;
    }

    public void setInstalledDate(Date installedDate) {
        this.installedDate = installedDate;
    }

    public Boolean getIsActive() {
        return isActive;
    }

    public void setIsActive(Boolean isActive) {
        this.isActive = isActive;
    }

    public Boolean getIsBillable() {
        return isBillable;
    }

    public void setIsBillable(Boolean isBillable) {
        this.isBillable = isBillable;
    }

    public Boolean getIsProrate() {
        return isProrate;
    }

    public void setIsProrate(Boolean isProrate) {
        this.isProrate = isProrate;
    }

    public Double getLobPrice() {
        return lobPrice;
    }

    public void setLobPrice(Double lobPrice) {
        this.lobPrice = lobPrice;
    }

    public Integer getMasterNumber() {
        return masterNumber;
    }

    public void setMasterNumber(Integer masterNumber) {
        this.masterNumber = masterNumber;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public String getPaymentType() {
        return paymentType;
    }

    public void setPaymentType(String paymentType) {
        this.paymentType = paymentType;
    }

    public Integer getPreviousMasterNumber() {
        return previousMasterNumber;
    }

    public void setPreviousMasterNumber(Integer previousMasterNumber) {
        this.previousMasterNumber = previousMasterNumber;
    }

    public String getProfitCenter() {
        return profitCenter;
    }

    public void setProfitCenter(String profitCenter) {
        this.profitCenter = profitCenter;
    }

    public String getSalesmanId() {
        return salesmanId;
    }

    public void setSalesmanId(String salesmanId) {
        this.salesmanId = salesmanId;
    }

    public Double getSellingPrice() {
        return sellingPrice;
    }

    public void setSellingPrice(Double sellingPrice) {
        this.sellingPrice = sellingPrice;
    }

    public Double getSetupFee() {
        return setupFee;
    }

    public void setSetupFee(Double setupFee) {
        this.setupFee = setupFee;
    }

    public Date getSoldDate() {
        return soldDate;
    }

    public void setSoldDate(Date soldDate) {
        this.soldDate = soldDate;
    }

    public Date getUpgradeDate() {
        return upgradeDate;
    }

    public void setUpgradeDate(Date upgradeDate) {
        this.upgradeDate = upgradeDate;
    }

    public Boolean getUpgradeDowngrade() {
        return upgradeDowngrade;
    }

    public void setUpgradeDowngrade(Boolean upgradeDowngrade) {
        this.upgradeDowngrade = upgradeDowngrade;
    }

    public String getVoxcontractDebitOrderDay() {
        return voxcontractDebitOrderDay;
    }

    public void setVoxcontractDebitOrderDay(String voxcontractDebitOrderDay) {
        this.voxcontractDebitOrderDay = voxcontractDebitOrderDay;
    }

    public String getVoxcontractItemBillingInfoId() {
        return voxcontractItemBillingInfoId;
    }

    public void setVoxcontractItemBillingInfoId(String voxcontractItemBillingInfoId) {
        this.voxcontractItemBillingInfoId = voxcontractItemBillingInfoId;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getVoxcontractId() {
        return voxcontractId;
    }

    public void setVoxcontractId(String voxcontractId) {
        this.voxcontractId = voxcontractId;
    }

    public String getVoxcontractProductId() {
        return voxcontractProductId;
    }

    public void setVoxcontractProductId(String voxcontractProductId) {
        this.voxcontractProductId = voxcontractProductId;
    }
    
}
