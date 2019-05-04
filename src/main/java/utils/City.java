package utils;

import java.io.Serializable;

public class City implements Serializable {
    private String city;
    private String latitude;
    private String longitude;

    public City(String city, String latitude, String longitude) {
        this.city = city;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getLatitude() {
        return latitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    public String getLongitude() {
        return longitude;
    }

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }
}
