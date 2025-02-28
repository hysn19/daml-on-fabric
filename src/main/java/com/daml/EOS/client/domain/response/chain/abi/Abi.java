package com.daml.EOS.client.domain.response.chain.abi;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Abi {

    private String accountName;

    private com.daml.EOS.client.domain.response.chain.code.Abi abi;


    public String getAccountName() {
        return accountName;
    }

    @JsonProperty("account_name")
    public void setAccountName(String accountName) {
        this.accountName = accountName;
    }

    public com.daml.EOS.client.domain.response.chain.code.Abi getAbi() {
        return abi;
    }

    public void setAbi(com.daml.EOS.client.domain.response.chain.code.Abi abi) {
        this.abi = abi;
    }

}
