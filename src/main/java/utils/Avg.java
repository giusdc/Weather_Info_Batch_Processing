package utils;

import java.io.Serializable;

public class Avg implements Serializable {

    private static final long serialVersionUID = 1L;

    private double sum;
    private int num;

    public Avg(double sum, int num) {
        this.sum = sum;
        this.num = num;
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