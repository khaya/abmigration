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
public class AccountServiceRequestNoPairComparator implements Comparator<AccountServiceRequestNoPair> {

    public int compare(AccountServiceRequestNoPair asrnp0, AccountServiceRequestNoPair asrnp1) {
        int result = asrnp0.getAccountId().compareTo(asrnp1.getAccountId());
        return (result == 0 ? asrnp0.getServiceRequestNo().compareTo(asrnp1.getServiceRequestNo()) : result);
    }

}
