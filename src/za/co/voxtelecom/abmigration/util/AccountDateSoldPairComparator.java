/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.voxtelecom.abmigration.util;

import java.util.Comparator;

/**
 *
 * @author khaya
 */
public class AccountDateSoldPairComparator implements Comparator<AccountDateSoldPair> {

    public int compare(AccountDateSoldPair adsp0, AccountDateSoldPair adsp1) {
        int result = adsp0.getAccountId().compareTo(adsp1.getAccountId());
        if ((adsp0.getDateSold() != null) && (adsp1.getDateSold() != null)) {
            return (result == 0 ? adsp0.getDateSold().compareTo(adsp1.getDateSold()) : result);
        } else {
            return result;
        }
    }
}
