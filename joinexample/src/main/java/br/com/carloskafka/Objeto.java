package br.com.carloskafka;

import java.io.Serializable;
import java.util.UUID;

public class Objeto implements Serializable {
    private String agencia;
    private String conta;
    private String numerMov;
    private String valorAleatorio;

    public Objeto() {
    }

    public Objeto(String agencia, String conta, String numerMov) {
        this.agencia = agencia;
        this.conta = conta;
        this.numerMov = numerMov;
        this.valorAleatorio = UUID.randomUUID().toString();
    }

    public String getAgencia() {
        return agencia;
    }

    public String getConta() {
        return conta;
    }

    public String getNumerMov() {
        return numerMov;
    }

    public String key() {
        return getAgencia() + getConta() + getNumerMov();
    }

    public String getValorAleatorio() {
        return valorAleatorio;
    }

    @Override
    public String toString() {
        return "Objeto{" +
                "agencia='" + agencia + '\'' +
                ", conta='" + conta + '\'' +
                ", numerMov='" + numerMov + '\'' +
                ", valorAleatorio='" + valorAleatorio + '\'' +
                ", key='" + key() + '\'' +
                '}';
    }
}
