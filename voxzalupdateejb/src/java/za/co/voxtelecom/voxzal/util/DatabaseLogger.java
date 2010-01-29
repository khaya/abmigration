/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.voxtelecom.voxzal.util;

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
 * 
 */
public class DatabaseLogger {

    private static Connection voxzalConnection;
    private static PreparedStatement voxzalInsertPreparedStatement;

    static {
        voxzalConnection = getConnection("importerrorslog.config");
        if (voxzalConnection != null) {
        }
    }

    public static Connection getConnection(String fileName) {
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
            Logger.getLogger(DatabaseLogger.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(DatabaseLogger.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                is.close();
            } catch (IOException ex) {
                Logger.getLogger(DatabaseLogger.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return connection;
    }

    public DatabaseLogger() {
    }

    public static void main(String[] args) {
        DatabaseLogger dl = new DatabaseLogger();
        //test log :)
        dl.logError(2000, "dsadfadfa", "payment_option_id", 3, 0,"payment_option_id_strace", "blah blah blah blah");
    }

    public void log(Integer masterNumberId, String voxcontractId, Integer sourceId, Integer sourceDatabaseId, Integer regionId, Integer paymentOptionId, Integer profitCenterId) {
    }

    public void logError(Integer masterNumberId, String voxcontractId, String columnName, Integer columnValue, Integer imported, String columnStrace, String stackTrace) {
        try {
            voxzalInsertPreparedStatement = voxzalConnection.prepareStatement("insert into big_blue_import_errors_log(master_number_id,voxcontract_id, " + columnName + "," + columnStrace + ",imported) values(?,?,?,?,?) on duplicate key update " + columnName + " = ?," + columnStrace + "= ?");
            voxzalInsertPreparedStatement.setInt(1, masterNumberId);
            voxzalInsertPreparedStatement.setString(2, voxcontractId);
            voxzalInsertPreparedStatement.setInt(3, columnValue);
            voxzalInsertPreparedStatement.setString(4, stackTrace);
            voxzalInsertPreparedStatement.setInt(5, imported);
            voxzalInsertPreparedStatement.setInt(6, columnValue);
            voxzalInsertPreparedStatement.setString(7, stackTrace);
            voxzalInsertPreparedStatement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseLogger.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            close(voxzalInsertPreparedStatement);
        }
    }

    public void logImport(Integer masterNumberId, String customerId, String accountCode, String customerName, String globalCustomerGuid, String systemId, Integer accountId, String action) {
        try {
            voxzalInsertPreparedStatement = voxzalConnection.prepareStatement("insert into global_customer_import_log(master_number_id,customer_id,account_code,customer_name,global_customer_guid,system_id,account_id,action) values(?,?,?,?,?,?,?,?) on duplicate key update   customer_id= ? , account_code=?,customer_name =?");
            voxzalInsertPreparedStatement.setInt(1, masterNumberId);
            voxzalInsertPreparedStatement.setString(2, customerId);
            voxzalInsertPreparedStatement.setString(3, accountCode);
            voxzalInsertPreparedStatement.setString(4, customerName);
            voxzalInsertPreparedStatement.setString(5, globalCustomerGuid);
            voxzalInsertPreparedStatement.setString(6, systemId);
            voxzalInsertPreparedStatement.setInt(7, accountId);
            voxzalInsertPreparedStatement.setString(8, action);
            voxzalInsertPreparedStatement.setString(9, customerId);
            voxzalInsertPreparedStatement.setString(10, accountCode);
            voxzalInsertPreparedStatement.setString(11, customerName);
            voxzalInsertPreparedStatement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseLogger.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            close(voxzalInsertPreparedStatement);
        }
    }

    //logging an update
    public void logImport(Integer masterNumberId, String customerId, String accountCode, String customerName, Integer accountId, String action) {
        try {
            voxzalInsertPreparedStatement = voxzalConnection.prepareStatement("insert into global_customer_import_log(master_number_id,customer_id,account_code,customer_name,account_id,action) values(?,?,?,?,?,?) on duplicate key update   customer_id= ? , account_code=?,customer_name =?");
            voxzalInsertPreparedStatement.setInt(1, masterNumberId);
            voxzalInsertPreparedStatement.setString(2, customerId);
            voxzalInsertPreparedStatement.setString(3, accountCode);
            voxzalInsertPreparedStatement.setString(4, customerName);
            voxzalInsertPreparedStatement.setInt(5, accountId);
            voxzalInsertPreparedStatement.setString(6, action);
            voxzalInsertPreparedStatement.setString(7, customerId);
            voxzalInsertPreparedStatement.setString(8, accountCode);
            voxzalInsertPreparedStatement.setString(9, customerName);
            voxzalInsertPreparedStatement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseLogger.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            close(voxzalInsertPreparedStatement);
        }
    }

    private void close(PreparedStatement preparedStatement) {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseLogger.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void close(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(DatabaseLogger.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
