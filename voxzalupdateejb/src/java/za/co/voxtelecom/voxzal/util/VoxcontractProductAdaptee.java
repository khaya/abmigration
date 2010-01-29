/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package za.co.voxtelecom.voxzal.util;

/**
 *
 * @author khaya
 */
public class VoxcontractProductAdaptee {

    private String voxcontractProductId;
    private String configDeviceInfo;
    private String lobId;
    private String lobProductId;
    private String name;
    private String productCode;
    private String planType;
    private String voxcontractProductPackage;

    public VoxcontractProductAdaptee() {
    }

    public VoxcontractProductAdaptee(String voxcontractProductId, String configDeviceInfo, String lobId, String lobProductId, String name, String productCode, String planType, String voxcontractProductPackage) {
        this.voxcontractProductId = voxcontractProductId;
        this.configDeviceInfo = configDeviceInfo;
        this.lobId = lobId;
        this.lobProductId = lobProductId;
        this.name = name;
        this.productCode = productCode;
        this.planType = planType;
        this.voxcontractProductPackage = voxcontractProductPackage;
    }

    public String getConfigDeviceInfo() {
        return configDeviceInfo;
    }

    public void setConfigDeviceInfo(String configDeviceInfo) {
        this.configDeviceInfo = configDeviceInfo;
    }

    public String getLobId() {
        return lobId;
    }

    public void setLobId(String lobId) {
        this.lobId = lobId;
    }

    public String getLobProductId() {
        return lobProductId;
    }

    public void setLobProductId(String lobProductId) {
        this.lobProductId = lobProductId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPlanType() {
        return planType;
    }

    public void setPlanType(String planType) {
        this.planType = planType;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getVoxcontractProductId() {
        return voxcontractProductId;
    }

    public void setVoxcontractProductId(String voxcontractProductId) {
        this.voxcontractProductId = voxcontractProductId;
    }

    public String getVoxcontractProductPackage() {
        return voxcontractProductPackage;
    }

    public void setVoxcontractProductPackage(String voxcontractProductPackage) {
        this.voxcontractProductPackage = voxcontractProductPackage;
    }
}
