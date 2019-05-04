package utils;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Locale;

public class WeatherInfo implements Serializable {

    private String date;
    private String description;
    private String city;

    public WeatherInfo(String date, String description, String city) {
        this.date = date;
        this.description = description;
        this.city = city;
    }

    public LocalDate getDate() throws ParseException {
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd hh", Locale.US);
        Date date2 = format.parse(date);
        LocalDate ld=date2.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        return ld;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String  getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }
}
