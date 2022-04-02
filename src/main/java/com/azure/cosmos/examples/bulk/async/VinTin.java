package com.azure.cosmos.examples.bulk.async;

import java.io.Serializable;

public class VinTin implements Serializable {
    private final String vin;
    private String tin;
    private String id;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public VinTin(String vin) {
        this.vin = vin;
    }

    public String getVin() {
        return vin;
    }

    public String getTin() {
        return tin;
    }

    public void setTin(String tin) {
        this.tin = tin;
        setId(String.format("%s:%s", vin, tin));
    }
}
