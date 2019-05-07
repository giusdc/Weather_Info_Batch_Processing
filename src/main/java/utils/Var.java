package utils;

import java.io.Serializable;

public class Var implements Serializable {

    private double value;
    private int n;

    public Var(double value, int n) {
        this.value = value;
        this.n = n;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public double getVar(){

        return Math.sqrt(this.value/this.n);
    }
}
