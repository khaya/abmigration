/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.voxtelecom.abmigration.util;

import java.sql.Date;

/**
 *
 * @author khaya
 */
public class AccountDateSoldPair {

    private String accountId;
    private Date dateSold;

    public AccountDateSoldPair(String accountId, Date dateSold) {
        this.accountId = accountId;
        this.dateSold = dateSold;
    }

    public AccountDateSoldPair() {
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public Date getDateSold() {
        return dateSold;
    }

    public void setDateSold(Date dateSold) {
        this.dateSold = dateSold;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AccountDateSoldPair other = (AccountDateSoldPair) obj;
        if ((this.accountId == null) ? (other.accountId != null) : !this.accountId.equals(other.accountId)) {
            return false;
        }
        if (this.dateSold != other.dateSold && (this.dateSold == null || !this.dateSold.equals(other.dateSold))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (this.accountId != null ? this.accountId.hashCode() : 0);
        hash = 31 * hash + (this.dateSold != null ? this.dateSold.hashCode() : 0);
        return hash;
    }

    
}
