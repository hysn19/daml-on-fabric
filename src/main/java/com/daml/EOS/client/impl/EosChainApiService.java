package com.daml.EOS.client.impl;

import com.daml.EOS.client.domain.request.chain.AbiJsonToBinRequest;
import com.daml.EOS.client.domain.request.chain.RequiredKeysRequest;
import com.daml.EOS.client.domain.request.chain.transaction.PushTransactionRequest;
import com.daml.EOS.client.domain.response.chain.*;
import com.daml.EOS.client.domain.response.chain.abi.Abi;
import com.daml.EOS.client.domain.response.chain.account.Account;
import com.daml.EOS.client.domain.response.chain.code.Code;
import com.daml.EOS.client.domain.response.chain.currencystats.CurrencyStats;
import com.daml.EOS.client.domain.response.chain.transaction.PushedTransaction;
import com.daml.EOS.client.domain.response.chain.transaction.ScheduledTransactionResponse;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.POST;

import java.util.List;
import java.util.Map;

public interface EosChainApiService {

    @GET("/v1/chain/get_info")
    Call<ChainInfo> getChainInfo();

    @POST("/v1/chain/get_block")
    Call<Block> getBlock(@Body Map<String, String> requestFields);

    @POST("/v1/chain/get_account")
    Call<Account> getAccount(@Body Map<String, String> requestFields);

    @POST("/v1/chain/get_abi")
    Call<Abi> getAbi(@Body Map<String, String> requestFields);

    @POST("/v1/chain/get_code")
    Call<Code> getCode(@Body Map<String, String> requestFields);

    @POST("/v1/chain/get_table_rows")
    Call<TableRow> getTableRows(@Body Map<String, String> requestFields);

    @POST("/v1/chain/get_currency_balance")
    Call<List<String>> getCurrencyBalance(@Body Map<String, String> requestFields);

    @POST("/v1/chain/abi_json_to_bin")
    Call<AbiJsonToBin> abiJsonToBin(@Body AbiJsonToBinRequest abiJsonToBinRequest);

    @POST("/v1/chain/abi_bin_to_json")
    Call<AbiBinToJson> abiBinToJson(@Body Map<String, String> requestFields);

    @POST("/v1/chain/push_transaction")
    Call<PushedTransaction> pushTransaction(@Body PushTransactionRequest pushTransactionRequest);

    @Headers("Content-Type: application/json;charset=UTF-8")
    @POST("/v1/chain/push_transaction")
    Call<PushedTransaction> pushRawTransaction(@Body String tx);

    @POST("/v1/chain/push_transactions")
    Call<List<PushedTransaction>> pushTransactions(@Body List<PushTransactionRequest> pushTransactionRequests);

    @POST("/v1/chain/get_required_keys")
    Call<RequiredKeys> getRequiredKeys(@Body RequiredKeysRequest requiredKeysRequest);

    @POST("/v1/chain/get_currency_stats")
    Call<Map<String, CurrencyStats>> getCurrencyStats(@Body Map<String, String> requestFields);

    @POST("/v1/chain/get_scheduled_transactions")
    Call<ScheduledTransactionResponse> getScheduledtransaction(@Body Map<String, String> requestFields);

}
