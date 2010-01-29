/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.voxtelecom.voxzal.util;

import com.tcm.utils.guid.GuidGenerator;
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
public class VoxcontractAdapter {
    /*
     *
     *  properties for
     */

    private static Connection bigBlueConnection;
    private static Connection voxzalConnection;
    PreparedStatement bigBlueCustomerPreparedStatement;
    PreparedStatement bigBlueCancelReasonPreparedStatement;
    PreparedStatement bigBlueSalesStaffPreparedStatement;
    PreparedStatement bigBlueSourceBusinessPreparedStatement;
    PreparedStatement bigBlueDataSourceDataPreparedStatement;
    PreparedStatement bigBluePaymentPreparedStatement;
    PreparedStatement bigBlueProfiitPreparedStatement;
    PreparedStatement bigBlueRegionPreparedStatement;
    PreparedStatement bigBlueProductPreparedStatement;
    PreparedStatement voxzalCustomerPreparedStatement;
    PreparedStatement voxzalUsrPreparedStatement;
    PreparedStatement voxzalDataSourcePreparedStatement;
    PreparedStatement voxzalCancelPreparedStatement;
    PreparedStatement voxzalSourcePreparedStatement;
    PreparedStatement voxzalPaymentPreparedStatement;
    PreparedStatement voxzalProfitPreparedStatement;
    PreparedStatement voxzalRegionPreparedStatement;
    PreparedStatement voxzalLobPreparedStatement;
    private ResultSet bigBlueTempResultSet;
    private ResultSet bigBlueProductResultSet;
    private PreparedStatement voxzalPlanTypePreparedStatement;
    private PreparedStatement voxzalBaseProductPreparedStatement;
    private PreparedStatement voxzalLobProductPreparedStatement;
    private PreparedStatement voxzalGlobalCustomerPreparedStatement;
    private PreparedStatement voxzalReadGlobalCustomerIdPreparedStatement;
    private PreparedStatement voxzalReadGlobalCustomerPreparedStatement;
    private ResultSet voxzalTempResultSet;
    private boolean ignoreAnnuity;
    private int masterNumberId;
    private VoxcontractProductAdaptee voxcontractProductAdaptee;
    private static DatabaseLogger databaseLogger;

    static {
        bigBlueConnection = getConnection("bigblue.config");
        voxzalConnection = getConnection("voxzal.config");
        databaseLogger = new DatabaseLogger();
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
            Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
            //      errorLogger.info("FileNotFoundException getting connection for database : " + fileName);
        } catch (Exception ex) {
            Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
            //     errorLogger.info("Exception getting connection for database : " + fileName);
        } finally {
            try {
                is.close();
            } catch (IOException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return connection;
    }

    public VoxcontractAdapter() {
        System.out.println("Initialising Statements");
        try {
            bigBlueCustomerPreparedStatement = bigBlueConnection.prepareStatement("select BrilliantAccountNumber,CompanyName from Account where AccountID=?");
            bigBlueCancelReasonPreparedStatement = bigBlueConnection.prepareStatement("select CancellationReason from CancellationReason where CancellationReasonID=?");
            bigBlueSalesStaffPreparedStatement = bigBlueConnection.prepareStatement("select Code from SalesStaff where StaffID=?");
            bigBlueSourceBusinessPreparedStatement = bigBlueConnection.prepareStatement("select SourceOfBusiness from Source where SourceID=?");
            bigBluePaymentPreparedStatement = bigBlueConnection.prepareStatement("select PaymentOption from PaymentOption where PaymentOptionID=?");
            bigBlueProfiitPreparedStatement = bigBlueConnection.prepareStatement("select ProfitCentreCode from ProfitCentre where ProfitCentreID=?");
            bigBlueRegionPreparedStatement = bigBlueConnection.prepareStatement("select Region from Region where RegionID=?");
            bigBlueProductPreparedStatement = bigBlueConnection.prepareStatement("select ProductCode from Product where ProductID=?");

            bigBlueDataSourceDataPreparedStatement = bigBlueConnection.prepareStatement("select SourceOfData from SourceDatabase where SourceDatabaseID=?");



            voxzalCustomerPreparedStatement = voxzalConnection.prepareStatement("select id from customer where pastel_code=?");
            voxzalCancelPreparedStatement = voxzalConnection.prepareStatement("select cancellation_reason_id from cancellation_reason where cancellation_reason=?");
            voxzalUsrPreparedStatement = voxzalConnection.prepareStatement("select id from usr where usr_name=?");
            voxzalDataSourcePreparedStatement = voxzalConnection.prepareStatement("select id from voxcontract_datasource where name=?");
            voxzalSourcePreparedStatement = voxzalConnection.prepareStatement("select SourceID from voxcontract_source where SourceOfBusiness=?");
            voxzalPaymentPreparedStatement = voxzalConnection.prepareStatement("select id from payment_type where type_name=?");
            voxzalProfitPreparedStatement = voxzalConnection.prepareStatement("select profit_center_id from profit_center where name like ?");
            voxzalRegionPreparedStatement = voxzalConnection.prepareStatement("select region_id from region where name=?");
            voxzalReadGlobalCustomerPreparedStatement = voxzalConnection.prepareStatement("select customer_id from global_customer where account_code=?");
            voxzalReadGlobalCustomerIdPreparedStatement = voxzalConnection.prepareStatement("select customer_id from global_customer where customer_id=?");
            voxzalLobPreparedStatement = voxzalConnection.prepareStatement("select base_product_id,lob_id,lob_product_id,name from lob_product where lob_product_code=?");
            voxzalPlanTypePreparedStatement = voxzalConnection.prepareStatement("select plan_type_id from base_product where base_product_id=?");


        } catch (SQLException ex) {
            Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
            //  errorLogger.info("SQLException initialising statements ");
        }
    }

    public VoxcontractItemBillingInfoAdaptee createVoxcontractItemBillingInfoAdapter(ResultSet resultSet, String voxcontractId, String voxcontractProductId) {
        VoxcontractItemBillingInfoAdaptee vibia = new VoxcontractItemBillingInfoAdaptee();
        String voxcontractItemBillingInfoId = getId();
        try {
            masterNumberId = resultSet.getInt("masternumberid");
            //runs if master number is not in voxzal master number list
            vibia.setVoxcontractItemBillingInfoId(voxcontractItemBillingInfoId); // billing_info_id
            vibia.setBillingStartDate(resultSet.getDate("datesold")); //billing_start_date
            vibia.setCancellation(getBoolean(resultSet.getString("cancellation"))); //cancellation
            vibia.setCancelledDate(resultSet.getDate("cancellationdate")); //cancellation_date
            vibia.setCommisionableValue(resultSet.getDouble("costprice")); //commisionable_value
            vibia.setCorePrice(resultSet.getDouble("corebilling")); //core_price
            vibia.setInstalledDate(resultSet.getDate("installeddate")); //installed_date
            vibia.setIsActive(getBoolean(resultSet.getString("billable"))); //is_active
            vibia.setIsBillable(getBoolean(resultSet.getString("billable"))); //is_billable
            vibia.setIsProrate(getBoolean(resultSet.getString("proratabilled"))); //is_prorata
            vibia.setLobPrice(0.0d); //lob_price  where do i read it ?
            vibia.setMasterNumber(masterNumberId); //master_number
            vibia.setNote(resultSet.getString("notes")); //note
            vibia.setPreviousMasterNumber(getPreviousMasterNumber(resultSet.getString("oldmasternumber"))); //previous_master_number
            vibia.setSalesmanId(null); //salesman_id //TODO is it still necessary
            vibia.setSellingPrice(resultSet.getDouble("billings")); //selling_price
            vibia.setSetupFee(resultSet.getDouble("setupfee")); //setup_fee
            vibia.setSoldDate(resultSet.getDate("datesold")); //sold_date
            vibia.setUpgradeDate(resultSet.getDate("salesupgradedate")); //upgrade_date
            vibia.setUpgradeDowngrade(getBoolean(resultSet.getString("upgrade"))); //upgrade_downgrade
            vibia.setVoxcontractDebitOrderDay(getVoxcontractDebitOrderDay(resultSet.getString("paymentday"))); //voxcontract_debit_order_day ?????
            vibia.setAccountManager(getAccountManager(resultSet.getInt("salesmanid"))); //account_manager_id
            vibia.setCancellationReason(getCancellationReason(resultSet.getInt("cancellationreasonid"))); //cancellation_reason_id
            vibia.setPaymentType(getPaymentType(resultSet.getInt("paymentoptionid"))); //payment_type_id
            vibia.setProfitCenter(getProfitCenter(resultSet.getInt("profitcentreid"))); //profit_center_id
            vibia.setRegion(getRegion(resultSet.getInt("regionid"))); //region_id
            vibia.setVoxcontractId(voxcontractId);
            vibia.setVoxcontractProductId(voxcontractProductId);

        } catch (SQLException sqle) {
        }
        return vibia;
    }

    public VoxcontractAdaptee createVoxcontract(ResultSet resultSet) {
        String voxcontractId = getId();
        String voxcontractInfoId = getId();

        VoxcontractAdaptee va = new VoxcontractAdaptee();
        try {
               masterNumberId = resultSet.getInt("masternumberid");
            String globalCustomerId = getGlobalCustomer(resultSet.getInt("accountid"));
            va.setAccountManagerId(getAccountManager(resultSet.getInt("salesmanid")));
            va.setCustomerId(globalCustomerId);
            va.setDateEnded(null);
            va.setDateStarted(resultSet.getDate("datesold"));
            va.setDivision(null);
            va.setExpiryDate(null);
            va.setGroupCompany(getGroupCompany(resultSet.getString("companysourcecode")));
            va.setOriginalAccountManagerId(getAccountManager(resultSet.getInt("originalsalesmanid")));
            va.setResellerId(null);
            va.setTicketNumber(null);
            va.setVoxcontractDataSource(getVoxcontractDataSource(resultSet.getInt("sourcedatabaseid")));
            va.setVoxcontractId(voxcontractId);
            va.setVoxcontractInfoId(voxcontractInfoId);
            va.setVoxcontractPeriod(getVoxcontractPeriod());
            va.setVoxcontractSource(getVoxcontractSource(resultSet.getInt("sourceid")));
            va.setVoxcontractStatus(getVoxcontractStatus());
            va.setVoxcontractType(getVoxcontractType());
            createVoxcontractProduct(resultSet.getInt("productid"),voxcontractId);
        } catch (SQLException sqle) {
            return null;
        }
        return va;
    }

    private VoxcontractProductAdaptee createVoxcontractProduct(Integer productId,String voxcontractId) {
        VoxcontractProductAdaptee vpa = new VoxcontractProductAdaptee();
        String voxcontractProductId = getId();
        String productCode = null;
        try {

            bigBlueProductPreparedStatement.setInt(1, productId);
            bigBlueTempResultSet = bigBlueProductPreparedStatement.executeQuery();
            bigBlueTempResultSet.next();
            productCode = bigBlueTempResultSet.getString("ProductCode");

            voxzalLobPreparedStatement.setString(1, productCode);
            voxzalTempResultSet = voxzalLobPreparedStatement.executeQuery();
            voxzalTempResultSet.next();
            //if the resultset is empty dont create a base product and lob product
            // set ignoreAnnuity to true
            // and return
            if (!voxzalTempResultSet.isFirst()) {
                System.out.println("Skipping product : " + productCode);
                databaseLogger.logError(masterNumberId, voxcontractId, "product_id", productId, 0, "product_id_strace", "missing product");
                ignoreAnnuity = true;
                return null;

            }
            String baseProductId = voxzalTempResultSet.getString("base_product_id");
            String lobId = voxzalTempResultSet.getString("lob_id");
            String lobProductId = voxzalTempResultSet.getString("lob_product_id");
            String name = voxzalTempResultSet.getString("name");
            voxzalPlanTypePreparedStatement.setString(1, baseProductId);
            voxzalTempResultSet = voxzalPlanTypePreparedStatement.executeQuery();
            voxzalTempResultSet.next();
            String planType = voxzalTempResultSet.getString("plan_type_id");
            vpa.setVoxcontractProductId(voxcontractProductId);//voxcontractract_product_id
            vpa.setConfigDeviceInfo(null); //config_device_info
            vpa.setLobId(lobId); //lob_id
            vpa.setLobProductId(lobProductId);//lob_product_id
            vpa.setName(name);//name
            vpa.setProductCode(productCode);//product_code
            vpa.setPlanType(planType);//plan_type
            vpa.setVoxcontractProductPackage(null);//voxcontract_product_package_id
            voxcontractProductAdaptee = vpa;

        } catch (SQLException ex) {
            Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, productCode, ex);
        } finally {
            close(bigBlueTempResultSet);
            close(voxzalTempResultSet);
        }
        return vpa;
    }

    public VoxcontractProductAdaptee getVoxcontractProductAdaptee() {
        return voxcontractProductAdaptee;
    }

    private String getGroupCompany(String companySourceCode) {
        String groupCompanyId = "2311CBD7-5772-66A6-B6D7-01B3A1C0C43A"; //vox datapro
        if (companySourceCode.equals("VX")) {
            groupCompanyId = "3CA1EA00-A89C-ED57-0DE4-32154413E4CA"; // vox exchange
        } else if (companySourceCode.equals("A")) {
            groupCompanyId = "1E8C3AEB-4BBF-7E3D-360D-121216F10D19"; // atlantic
        } else if (companySourceCode.equals("O")) {
            groupCompanyId = "1E8C3AEB-4BBF-7E3D-360D-121216F10D19";//orion
        }
        return groupCompanyId;
    }

    private boolean getBoolean(String b) {
        boolean booleanResult = false;
        if (b != null && !b.equals("0")) {
            booleanResult = true;
        }
        return booleanResult;
    }

    private Integer getPreviousMasterNumber(String oldMasterNumber) {
        Integer previousMasterNumber = 0;
        try {
            previousMasterNumber = Integer.valueOf(oldMasterNumber);
        } catch (Exception ex) {
            //TODO log this exception so that invalid master numbers can be tracked
          //  databaseLogger.logError(masterNumberId, null, masterNumberId, masterNumberId, masterNumberId, masterNumberId, masterNumberId);
        }
        return previousMasterNumber;
    }

    private String getVoxcontractDebitOrderDay(String paymentday) {
        String voxcontractDebitOrderDay = "ONE";
        if (paymentday.equals("15")) {
            voxcontractDebitOrderDay = "FIFTEEN";
        } else if (paymentday.equals("25")) {
            voxcontractDebitOrderDay = "TWENTY_FIVE";
        } else if (paymentday.equals("30")) {
            voxcontractDebitOrderDay = "THIRTY";
        }
        return voxcontractDebitOrderDay;
    }

    private String getVoxcontractPeriod() {
        return "TWENTY_FOUR";
    }

    private String getVoxcontractStatus() {
        return "APPROVED";
    }

    private String getVoxcontractType() {
        return "EVERGREEN";
    }

    public String getId() {
        return GuidGenerator.getNext();
    }

    private String getAccountManager(Integer salesmanId) {
        String usrId = null;
        String code = null;
        if (salesmanId != 0) {
            try {

                bigBlueSalesStaffPreparedStatement.setInt(1, salesmanId);
                bigBlueTempResultSet = bigBlueSalesStaffPreparedStatement.executeQuery();
                bigBlueTempResultSet.next();
                code = bigBlueTempResultSet.getString("Code");
                if (code != null) {
                    code = code.equals("allie-sue") ? "AllieSue" : code;
                    code = code.equals("pauld") ? "jeand" : code;

                    voxzalUsrPreparedStatement.setString(1, code);
                    voxzalTempResultSet = voxzalUsrPreparedStatement.executeQuery();
                    voxzalTempResultSet.next();
                    usrId = voxzalTempResultSet.getString("id");
                }
            } catch (SQLException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, code + "\for SalesMan " + salesmanId, ex);
                //     errorLogger.info("SQLException  in getting Salesman : " + salesmanId + " for MasterNumber " + masterNumberId);
               databaseLogger.logError(masterNumberId, null, "sales_man_id", salesmanId, 1, "sales_man_id_strace", ex.toString()); //masternumberId,voxontractId,columnValue,columnName,imported,stackTrace
            } finally {
                close(bigBlueTempResultSet);
                close(voxzalTempResultSet);
            }
        }
        return usrId;
    }

    private String getCancellationReason(Integer cancellationReasonId) {
        String cancellationReason = null;
        if (cancellationReasonId != 0) {
            try {
                bigBlueCancelReasonPreparedStatement.setInt(1, cancellationReasonId);
                bigBlueTempResultSet = bigBlueCancelReasonPreparedStatement.executeQuery();
                bigBlueTempResultSet.next();
                String cr = bigBlueTempResultSet.getString("CancellationReason");

                voxzalCancelPreparedStatement.setString(1, cr);
                voxzalTempResultSet = voxzalCancelPreparedStatement.executeQuery();
                voxzalTempResultSet.next();
                cancellationReason = voxzalTempResultSet.getString("cancellation_reason_id");
            } catch (SQLException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
                //     errorLogger.info("SQLException  in getting cancellation reason : " + cancellationReasonId + " for MasterNumber " + masterNumberId);
                   databaseLogger.logError(masterNumberId, null, "cancellation_reason_id", cancellationReasonId, 1, "cancellation_reason_id_strace", ex.toString());
            } finally {
                close(bigBlueTempResultSet);
                close(voxzalTempResultSet);
            }
        }
        return cancellationReason;
    }

    private String getVoxcontractDataSource(Integer sourceDatabaseId) {
        String voxcontractDatasourceId = null;
        String sourceOfData = null;
        if (sourceDatabaseId != 0) {
            try {
                bigBlueDataSourceDataPreparedStatement.setInt(1, sourceDatabaseId);
                bigBlueTempResultSet = bigBlueDataSourceDataPreparedStatement.executeQuery();
                bigBlueTempResultSet.next();
                sourceOfData = bigBlueTempResultSet.getString("SourceOfData");
                voxzalDataSourcePreparedStatement.setString(1, sourceOfData);
                voxzalTempResultSet = voxzalDataSourcePreparedStatement.executeQuery();
                voxzalTempResultSet.next();
                voxcontractDatasourceId = voxzalTempResultSet.getString("id");
            } catch (SQLException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
                //errorLogger.info("SQLException  in getting Voxcontract Data Source : " + sourceDatabaseId + "\t" + sourceOfData + " for MasterNumber " + masterNumberId);
                // databaseLogger.logError(masterNumberId, voxcontractId, "source_database_id", sourceDatabaseId, 1, "source_database_id_strace", ex.toString());
            } finally {
                close(bigBlueTempResultSet);
                close(voxzalTempResultSet);
            }
        }
        return voxcontractDatasourceId;

    }

    private String getVoxcontractSource(Integer sourceId) {
        String voxcontractSourceId = null;
        String sourceOfBusiness = null;
        if (sourceId != 0) {
            try {
                bigBlueSourceBusinessPreparedStatement.setInt(1, sourceId);
                bigBlueTempResultSet = bigBlueSourceBusinessPreparedStatement.executeQuery();
                bigBlueTempResultSet.next();
                sourceOfBusiness = bigBlueTempResultSet.getString("SourceOfBusiness");
                voxzalSourcePreparedStatement.setString(1, sourceOfBusiness);
                voxzalTempResultSet = voxzalSourcePreparedStatement.executeQuery();
                voxzalTempResultSet.next();
                voxcontractSourceId = voxzalTempResultSet.getString("SourceID");
            } catch (SQLException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
                //errorLogger.info("SQLException  in getting Voxcontract Source  : " + sourceId + "\t " + sourceOfBusiness + " for MasterNumber " + masterNumberId);
                //databaseLogger.logError(masterNumberId, voxcontractId, "source_id", sourceId, 1, "source_id_strace", ex.toString());
            } finally {
                close(bigBlueTempResultSet);
                close(voxzalTempResultSet);
            }
        }
        return voxcontractSourceId;


    }

    private String getPaymentType(Integer paymentOptionId) {
        String paymentTypeId = null;
        String paymentOption = null;
        if (paymentOptionId != 0) {
            try {
                bigBluePaymentPreparedStatement.setInt(1, paymentOptionId);
                bigBlueTempResultSet = bigBluePaymentPreparedStatement.executeQuery();
                bigBlueTempResultSet.next();
                paymentOption = bigBlueTempResultSet.getString("PaymentOption");
                paymentOption = paymentOption.equals("Credit Cards") ? "Credit Card" : paymentOption;
                paymentOption = paymentOption.equals("Cheque/EFT") ? "Cheque" : paymentOption;
                paymentOption = paymentOption.equals("Prepaids") ? "PrePaid" : paymentOption;

                voxzalPaymentPreparedStatement.setString(1, paymentOption);
                voxzalTempResultSet = voxzalPaymentPreparedStatement.executeQuery();
                voxzalTempResultSet.next();
                paymentTypeId = voxzalTempResultSet.getString("id");
            } catch (SQLException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, paymentOption, ex);
                //         errorLogger.info("SQLException  in getting Payment Type : " + paymentOptionId + " for MasterNumber " + masterNumberId);
                       databaseLogger.logError(masterNumberId, null, "payment_option_id", paymentOptionId, 1, "payment_option_id_strace", ex.toString());
            } finally {
                close(bigBlueTempResultSet);
                close(voxzalTempResultSet);
            }
        }
        return paymentTypeId;
    }

    private String getProfitCenter(Integer profitCentreId) {
        String profitCenterId = null;
        if (profitCentreId != 0) {
            try {
                bigBlueProfiitPreparedStatement.setInt(1, profitCentreId);
                bigBlueTempResultSet = bigBlueProfiitPreparedStatement.executeQuery();
                bigBlueTempResultSet.next();
                String profitCentreCode = bigBlueTempResultSet.getString("ProfitCentreCode");
                voxzalProfitPreparedStatement.setString(1, profitCentreCode + "%");
                voxzalTempResultSet = voxzalProfitPreparedStatement.executeQuery();
                voxzalTempResultSet.next();
                profitCenterId = voxzalTempResultSet.getString("profit_center_id");
            } catch (SQLException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
                //        errorLogger.info("SQLException  in getting Profit Center : " + profitCentreId + " for MasterNumber " + masterNumberId);
                databaseLogger.logError(masterNumberId, null, "profit_center_id", profitCentreId, 1, "profit_center_id_strace", ex.toString());
            } finally {
                close(bigBlueTempResultSet);
                close(voxzalTempResultSet);
            }
        }
        return profitCenterId;

    }

    private String getRegion(Integer regionId) {
        String rid = null;
        if (regionId != 0) {
            try {
                bigBlueRegionPreparedStatement.setInt(1, regionId);
                bigBlueTempResultSet = bigBlueRegionPreparedStatement.executeQuery();
                bigBlueTempResultSet.next();
                String region = bigBlueTempResultSet.getString("Region");
                voxzalRegionPreparedStatement.setString(1, region);
                voxzalTempResultSet = voxzalRegionPreparedStatement.executeQuery();
                voxzalTempResultSet.next();
                rid = voxzalTempResultSet.getString("region_id");
            } catch (SQLException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
                //          errorLogger.info("SQLException  in getting Region : " + regionId + " for MasterNumber " + masterNumberId);
                databaseLogger.logError(masterNumberId,null, "region_id", regionId, 1, "region_id_strace", ex.toString());
            } finally {
                close(bigBlueTempResultSet);
                close(voxzalTempResultSet);
            }
        }
        return rid;
    }

    private String getGlobalCustomer(Integer accountId) {
        System.out.println(" Getting a GlobalCustomer ");
        String customerId = null;
        String pastelCode = null;
        String companyName = null;
        try {

            bigBlueCustomerPreparedStatement.setInt(1, accountId);
            bigBlueTempResultSet = bigBlueCustomerPreparedStatement.executeQuery();
            bigBlueTempResultSet.next();
            pastelCode = bigBlueTempResultSet.getString("BrilliantAccountNumber");
            companyName = bigBlueTempResultSet.getString("CompanyName");
            System.out.println("have pastel code" + pastelCode);
            System.out.println("have company name" + companyName);
            voxzalReadGlobalCustomerPreparedStatement.setString(1, pastelCode);
            voxzalTempResultSet = voxzalReadGlobalCustomerPreparedStatement.executeQuery();
            voxzalTempResultSet.next();
            if (voxzalTempResultSet.isFirst()) {
                customerId = voxzalTempResultSet.getString("customer_id");
            } else {
                System.out.println("No record for :" + pastelCode);
                voxzalCustomerPreparedStatement.setString(1, pastelCode);
                voxzalTempResultSet = voxzalCustomerPreparedStatement.executeQuery();
                //if(voxzalTempResultSet.last())
                voxzalTempResultSet.next();
                if (voxzalTempResultSet.isFirst()) {
                    // if there is a matching customer search for the global customer and update it
                    customerId = voxzalTempResultSet.getString("id");
                    voxzalReadGlobalCustomerIdPreparedStatement.setString(1, customerId);
                    voxzalTempResultSet = voxzalReadGlobalCustomerIdPreparedStatement.executeQuery();
                    voxzalTempResultSet.next();
                    if (voxzalTempResultSet.isFirst()) {
                        //if global customer exists update the global customer
                        updateGlobalCustomer(customerId, companyName, pastelCode, accountId);
                    } else {
                        //if global customer doesnt exist add a new global customer
                        customerId = getId();
                        addGlobalCustomer(customerId, customerId, pastelCode, accountId);
                    }

                } else {
                    // if there is no customer create a  global customer and log it
                    customerId = getId();
                    addGlobalCustomer(customerId, customerId, pastelCode, accountId);
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, pastelCode, ex);
            //          errorLogger.info("SQLException  in getting Global Customer : " + accountId + " for MasterNumber " + masterNumberId);
//            databaseLogger.logError(masterNumberId, voxcontractId, "account_id", accountId, 1, "account_id_strace", ex.toString());
        } finally {
//            close(bigBlueTempResultSet);
//            close(voxzalTempResultSet);
        }
        if (customerId == null) {
            System.out.println(" ***** Customer Id is Null ****** for MasterNumberId " + masterNumberId);
        }
        return customerId;


    }

    private void updateGlobalCustomer(String customerId, String customerName, String accountCode, Integer accountId) {
        //       importLogger.info("Updating a  Global Customer " + customerName + "for Master Number " + masterNumberId);
        //      databaseLogger.logImport(masterNumberId, customerId, accountCode, customerName, accountId, "update");
        try {
            voxzalGlobalCustomerPreparedStatement = voxzalConnection.prepareStatement("update global_customer set account_code=?,customer_name=? where customer_id=?");
            voxzalGlobalCustomerPreparedStatement.setString(1, customerId);
            voxzalGlobalCustomerPreparedStatement.setString(2, customerName);
            voxzalGlobalCustomerPreparedStatement.setString(3, accountCode);
            voxzalGlobalCustomerPreparedStatement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
            //       errorLogger.info("SQLException  in adding Global Customer : " + customerId + " for MasterNumber " + masterNumberId);
            //      databaseLogger.logError(masterNumberId, voxcontractId, "account_id", accountId, 1, "account_id_strace", ex.toString());
        } finally {
            try {
                voxzalGlobalCustomerPreparedStatement.close();
            } catch (SQLException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private void addGlobalCustomer(String customerId, String customerName, String accountCode, Integer accountId) {
        String systemId = "E4C79946-CG62-1832-79A4-69B3F159EF47";
        String globalCustomerGuid = getId();
        //     importLogger.info("Adding a New Global Customer " + customerName + "for Master Number " + masterNumberId);

        databaseLogger.logImport(masterNumberId, customerId, accountCode, customerName, globalCustomerGuid, systemId, accountId, "new");
        try {
            voxzalGlobalCustomerPreparedStatement = voxzalConnection.prepareStatement("insert into global_customer(customer_id,customer_name,account_code,customer_global_guid,system_id) values(?,?,?,?,?)");
            voxzalGlobalCustomerPreparedStatement.setString(1, customerId);
            voxzalGlobalCustomerPreparedStatement.setString(2, customerName);
            voxzalGlobalCustomerPreparedStatement.setString(3, accountCode);
            voxzalGlobalCustomerPreparedStatement.setString(4, globalCustomerGuid);
            voxzalGlobalCustomerPreparedStatement.setString(5, systemId);
            voxzalGlobalCustomerPreparedStatement.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
            //     errorLogger.info("SQLException  in adding Global Customer : " + customerId + " for MasterNumber " + masterNumberId);
             databaseLogger.logError(masterNumberId, null, "account_id", accountId, 1, "account_id_strace", ex.toString());
        } finally {
            try {
                voxzalGlobalCustomerPreparedStatement.close();
            } catch (SQLException ex) {
                Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

    }

    private void close(PreparedStatement preparedStatement) {
        try {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void close(ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
        } catch (SQLException ex) {
            Logger.getLogger(VoxcontractAdapter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void close() {
        cleanUp();
    }

    private void cleanUp() {
        System.out.println("close all prepared read statements and free resources");
        close(bigBlueCustomerPreparedStatement);
        close(bigBlueCancelReasonPreparedStatement);
        close(bigBlueSalesStaffPreparedStatement);
        close(bigBlueDataSourceDataPreparedStatement);
        close(bigBluePaymentPreparedStatement);
        close(bigBlueProfiitPreparedStatement);
        close(bigBlueRegionPreparedStatement);
        close(bigBlueProductPreparedStatement);

        close(voxzalCustomerPreparedStatement);
        close(voxzalCancelPreparedStatement);
        close(voxzalUsrPreparedStatement);
        close(voxzalDataSourcePreparedStatement);
        close(voxzalSourcePreparedStatement);
        close(voxzalPaymentPreparedStatement);
        close(voxzalProfitPreparedStatement);
        close(voxzalRegionPreparedStatement);
        close(voxzalGlobalCustomerPreparedStatement);
        close(voxzalLobPreparedStatement);
        close(voxzalPlanTypePreparedStatement);
        close(voxzalReadGlobalCustomerPreparedStatement);
        close(voxzalReadGlobalCustomerIdPreparedStatement);

        try {
            if (bigBlueConnection != null) {
                bigBlueConnection.close();
            }
        } catch (Exception e) {
        }

    }
}
