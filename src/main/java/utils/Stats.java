package utils;

import java.io.Serializable;

public class Stats implements Serializable {

    private static final long serialVersionUID = 1L;

    private double sum;
    private int num;
    private double min;
    private double max;
    private double sumSquare;

    public Stats(double sum, int num, double sumSquare, double min, double max) {
        this.sum = sum;
        this.num = num;
        this.min = min;
        this.max = max;
        this.sumSquare=sumSquare;
    }

    public double getVar(){

        return Math.sqrt((this.getSumSquare()-this.getNum()*Math.pow(this.getAvg(),2))/(this.getNum()-1));

    }

    public double computeMin(double x){

        this.setMin(Math.min(this.min,x));
        return  this.min;
    }

    public double computeMax(double x){

        this.setMax(Math.max(this.max,x));
        return  this.max;
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

    public String getValues(){

       return String.valueOf(this.getAvg())+","+String.valueOf(this.getVar())+","+String.valueOf(this.getMin())+","+String.valueOf(this.getMax());
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
}