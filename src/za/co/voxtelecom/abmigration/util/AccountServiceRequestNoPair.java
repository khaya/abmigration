/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.voxtelecom.abmigration.util;

/**
 *
 * @author khaya
 */
public class AccountServiceRequestNoPair {

    private String accountId;
    private String serviceRequestNo;

    public AccountServiceRequestNoPair() {
    }

    public AccountServiceRequestNoPair(String accountId, String serviceRequestNo) {
        this.accountId = accountId;
        this.serviceRequestNo = serviceRequestNo;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getServiceRequestNo() {
        return serviceRequestNo;
    }

    public void setServiceRequestNo(String serviceRequestNo) {
        this.serviceRequestNo = serviceRequestNo;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AccountServiceRequestNoPair other = (AccountServiceRequestNoPair) obj;
        if ((this.accountId == null) ? (other.accountId != null) : !this.accountId.equals(other.accountId)) {
            return false;
        }
        if ((this.serviceRequestNo == null) ? (other.serviceRequestNo != null) : !this.serviceRequestNo.equals(other.serviceRequestNo)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 79 * hash + (this.accountId != null ? this.accountId.hashCode() : 0);
        hash = 79 * hash + (this.serviceRequestNo != null ? this.serviceRequestNo.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {
        return "Account Id " + accountId + "\tServiceRequestNo\t"+serviceRequestNo;
    }

}
