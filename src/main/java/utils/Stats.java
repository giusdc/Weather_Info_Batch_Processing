package utils;

import java.io.Serializable;
import java.util.*;

public class Stats implements Serializable {

    private static final long serialVersionUID = 1L;

    private double sum;
    private int num;
    private double min;
    private double max;
    private double sumSquare;
    private ArrayList<String> rank;

    public Stats(double sum, int num, double sumSquare, double min, double max) {
        this.sum = sum;
        this.num = num;
        this.min = min;
        this.max = max;
        this.sumSquare = sumSquare;
    }

    public Stats(double sum, int num) {
        this.sum = sum;
        this.num = num;
    }

    public Stats(ArrayList<String> rank) {
        this.rank = rank;
    }

    public double getVar() {

        return Math.sqrt((this.getSumSquare() - this.getNum() * Math.pow(this.getAvg(), 2)) / (this.getNum() - 1));

    }

    public double computeMin(double x) {

        this.setMin(Math.min(this.min, x));
        return this.min;
    }

    public double computeMax(double x) {

        this.setMax(Math.max(this.max, x));
        return this.max;
    }

    public String getValues() {

        return String.valueOf(this.getAvg()) + "," + String.valueOf(this.getVar()) + "," + String.valueOf(this.getMin()) + "," + String.valueOf(this.getMax());
    }

    public String computeRank() {


        String[] rankArray = this.rank.toArray(new String[0]);
        HashMap<Double, String> hashMap = new HashMap<>();

        for (int x = 0; x < rankArray.length; x++) {
            hashMap.put(Double.parseDouble(rankArray[x].split("_")[0]), rankArray[x].split("_")[1]);
        }

        TreeMap map = new TreeMap<>(hashMap);
        List<String> cities = new ArrayList<>(map.values());
        String result = "";

        for (int i = cities.size()-1; i > 0; i--) {
            result += cities.get(i)+",";
            if (i == cities.size() - 3)
                return result;
        }
        return result;
    }

    public ArrayList<String> addElement(String diff) {
        if (this.rank == null) {
            this.rank = new ArrayList<>();
        }
        this.rank.add(diff);
        return this.rank;
    }

    public double getSumSquare() {
        return sumSquare;
    }

    public void setSumSquare(double sumSquare) {
        this.sumSquare = sumSquare;
    }

    public void setMin(double min) {
        this.min = min;
    }


    public void setMax(double max) {
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }


    public double getAvg() {
        return (this.sum / this.num);
    }

    public double getSum() {
        return this.sum;
    }

    public int getNum() {
        return this.num;
    }

    public ArrayList<String> getRank() {
        return rank;
    }

    public void setRank(ArrayList<String> rank) {
        this.rank = rank;
    }
}





