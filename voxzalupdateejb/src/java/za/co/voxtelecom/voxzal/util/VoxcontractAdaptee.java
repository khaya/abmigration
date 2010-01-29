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
public class VoxcontractAdaptee {

    private String voxcontractId;
    private Date dateEnded;
    private String ticketNumber;
    private String division;
    private String customerId;
    private String voxcontractInfoId;
    private Date dateStarted;
    private String accountManagerId;
    private String originalAccountManagerId;
    private Date expiryDate;
    private String voxcontractPeriod;
    private String voxcontractStatus;
    private String voxcontractType;
    private String groupCompany;
    private String resellerId;
    private String voxcontractDataSource;
    private String voxcontractSource;

    public VoxcontractAdaptee() {
    }

    public VoxcontractAdaptee(String voxcontractId, Date dateEnded, String ticketNumber, String division, String customerId, String voxcontractInfoId, Date dateStarted, String accountManagerId, String originalAccountManagerId, Date expiryDate, String voxcontractPeriod, String voxcontractStatus, String voxcontractType, String groupCompany, String resellerId, String voxcontractDataSource, String voxcontractSource) {
        this.voxcontractId = voxcontractId;
        this.dateEnded = dateEnded;
        this.ticketNumber = ticketNumber;
        this.division = division;
        this.customerId = customerId;
        this.voxcontractInfoId = voxcontractInfoId;
        this.dateStarted = dateStarted;
        this.accountManagerId = accountManagerId;
        this.originalAccountManagerId = originalAccountManagerId;
        this.expiryDate = expiryDate;
        this.voxcontractPeriod = voxcontractPeriod;
        this.voxcontractStatus = voxcontractStatus;
        this.voxcontractType = voxcontractType;
        this.groupCompany = groupCompany;
        this.resellerId = resellerId;
        this.voxcontractDataSource = voxcontractDataSource;
        this.voxcontractSource = voxcontractSource;
    }

    public String getAccountManagerId() {
        return accountManagerId;
    }

    public void setAccountManagerId(String accountManagerId) {
        this.accountManagerId = accountManagerId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public Date getDateEnded() {
        return dateEnded;
    }

    public void setDateEnded(Date dateEnded) {
        this.dateEnded = dateEnded;
    }

    public Date getDateStarted() {
        return dateStarted;
    }

    public void setDateStarted(Date dateStarted) {
        this.dateStarted = dateStarted;
    }

    public String getDivision() {
        return division;
    }

    public void setDivision(String division) {
        this.division = division;
    }

    public Date getExpiryDate() {
        return expiryDate;
    }

    public void setExpiryDate(Date expiryDate) {
        this.expiryDate = expiryDate;
    }

    public String getGroupCompany() {
        return groupCompany;
    }

    public void setGroupCompany(String groupCompany) {
        this.groupCompany = groupCompany;
    }

    public String getOriginalAccountManagerId() {
        return originalAccountManagerId;
    }

    public void setOriginalAccountManagerId(String originalAccountManagerId) {
        this.originalAccountManagerId = originalAccountManagerId;
    }

    public String getResellerId() {
        return resellerId;
    }

    public void setResellerId(String resellerId) {
        this.resellerId = resellerId;
    }

    public String getTicketNumber() {
        return ticketNumber;
    }

    public void setTicketNumber(String ticketNumber) {
        this.ticketNumber = ticketNumber;
    }

    public String getVoxcontractDataSource() {
        return voxcontractDataSource;
    }

    public void setVoxcontractDataSource(String voxcontractDataSource) {
        this.voxcontractDataSource = voxcontractDataSource;
    }

    public String getVoxcontractId() {
        return voxcontractId;
    }

    public void setVoxcontractId(String voxcontractId) {
        this.voxcontractId = voxcontractId;
    }

    public String getVoxcontractInfoId() {
        return voxcontractInfoId;
    }

    public void setVoxcontractInfoId(String voxcontractInfoId) {
        this.voxcontractInfoId = voxcontractInfoId;
    }

    public String getVoxcontractPeriod() {
        return voxcontractPeriod;
    }

    public void setVoxcontractPeriod(String voxcontractPeriod) {
        this.voxcontractPeriod = voxcontractPeriod;
    }

    public String getVoxcontractSource() {
        return voxcontractSource;
    }

    public void setVoxcontractSource(String voxcontractSource) {
        this.voxcontractSource = voxcontractSource;
    }

    public String getVoxcontractStatus() {
        return voxcontractStatus;
    }

    public void setVoxcontractStatus(String voxcontractStatus) {
        this.voxcontractStatus = voxcontractStatus;
    }

    public String getVoxcontractType() {
        return voxcontractType;
    }

    public void setVoxcontractType(String voxcontractType) {
        this.voxcontractType = voxcontractType;
    }



}
