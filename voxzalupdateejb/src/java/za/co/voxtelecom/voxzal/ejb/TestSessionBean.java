/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package za.co.voxtelecom.voxzal.ejb;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.ejb.Schedule;
import javax.ejb.Stateless;
import javax.sql.DataSource;

/**
 *
 * @author khaya
 */
@Stateless
public class TestSessionBean implements TestSessionBeanLocal {
@Resource
private DataSource dataSource;
private Connection connection;

@PostConstruct
private  void initialise(){
        try {
            connection = dataSource.getConnection();
        } catch (SQLException ex) {
            Logger.getLogger(TestSessionBean.class.getName()).log(Level.SEVERE, null, ex);
        }
}

@PreDestroy
private void cleanUp(){

}
    @Schedule(minute="*/5")
    public void testMethod() {
        System.out.println("Test method running at  :" +  new Date());
    }
    
    // Add business logic below. (Right-click in editor and choose
    // "Insert Code > Add Business Method")
 
}
