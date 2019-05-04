package utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class CountryMap {



        /*public static void main(String[] args) throws Exception {

            HttpURLConnectionExample http = new HttpURLConnectionExample();

            System.out.println("Testing 1 - Send Http GET request");
            http.sendGet();

            //System.out.println("\nTesting 2 - Send Http POST request");
            //http.sendPost();

        }*/

    // HTTP GET request
    public static String sendGet(String line) throws Exception {

        //String url2 = "https://nominatim.openstreetmap.org/search.php?q=brandenburger+tor%2C+berlin%2C+deutschland&amp;format=json";


        City city= CityParser.parseCsv(line);


        String url="https://nominatim.openstreetmap.org/reverse?format=json&lat="+city.getLatitude()+"&lon="+city.getLongitude()+"&accept-language=en";

        //45.523449,-122.676208

        //31.769039,35.216331
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");
        con.addRequestProperty("REFERER", "https://nominatim.openstreetmap.org");

        //add request header
        con.setRequestProperty("User-Agent", "Mozilla/5.0");

        int responseCode = con.getResponseCode();
        System.out.println("\nSending 'GET' request to URL : " + url);
        System.out.println("Response Code : " + responseCode);

        BufferedReader in = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }

        JsonObject jsonObject= (JsonObject) new JsonParser().parse(String.valueOf(response));
        JsonObject address= (JsonObject) jsonObject.get("address");
        String country= String.valueOf(address.get("country"));




        in.close();

        //print result
        System.out.println(response.toString());
        return country;

    }

}
