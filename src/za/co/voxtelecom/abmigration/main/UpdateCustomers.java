/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.voxtelecom.abmigration.main;

import za.co.voxtelecom.abmigration.main.AnnuityMigration;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author khaya
 */
public class UpdateCustomers {

    private Connection voxzalConnection;
    private PreparedStatement voxzalSelectPreparedStatement;
    private PreparedStatement voxzalUpdatePreparedStatement;
    private ResultSet voxzalSelectResultSet;

    public Connection getConnection(String fileName) {
        Connection connection = null;
        InputStream is = null;
        Properties prop = null;
        try {
            prop = new Properties();
            is = new FileInputStream(fileName);
            prop.load(is);
            String url = prop.getProperty("url");
            connection = DriverManager.getConnection(url, prop);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AnnuityMigration.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(AnnuityMigration.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                is.close();
            } catch (IOException ex) {
                Logger.getLogger(AnnuityMigration.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return connection;
    }

    private void initilisePreparedStatements() {
        try {
            voxzalSelectPreparedStatement = voxzalConnection.prepareStatement("select c.id,c.customer_name as cCustomerName,c.pastel_code ,gc.customer_name as gcCustomerName,gc.account_code from customer c inner join global_customer gc on c.id=gc.customer_id where gc.account_code is null");
            voxzalSelectResultSet = voxzalSelectPreparedStatement.executeQuery();
            voxzalUpdatePreparedStatement = voxzalConnection.prepareStatement("update global_customer set account_code=?,customer_name=? where customer_id=?");
        } catch (SQLException ex) {
            Logger.getLogger(UpdateCustomers.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void setPreparedStatementsValues() {
        try {
            voxzalUpdatePreparedStatement.setString(1, voxzalSelectResultSet.getString("pastel_code")); // set account_code
            voxzalUpdatePreparedStatement.setString(2, voxzalSelectResultSet.getString("cCustomerName")); //set customer_name
            voxzalUpdatePreparedStatement.setString(3, voxzalSelectResultSet.getString("id"));
            voxzalUpdatePreparedStatement.addBatch();
        } catch (SQLException ex) {
            Logger.getLogger(UpdateCustomers.class.getName()).log(Level.SEVERE, null, ex);
        }


    }

    public static void main(String[] args) {
      
        UpdateCustomers uc = new UpdateCustomers();
        uc.voxzalConnection = uc.getConnection("voxzal.config");
        if (uc.voxzalConnection != null) {
            try {
                uc.initilisePreparedStatements();
                
                while (uc.voxzalSelectResultSet.next()) {
                    uc.setPreparedStatementsValues();
                }
                uc.voxzalUpdatePreparedStatement.executeBatch();
            } catch (SQLException ex) {
                Logger.getLogger(UpdateCustomers.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
