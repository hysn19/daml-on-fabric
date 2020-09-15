package com.daml.EOS;

import com.daml.EOS.client.EosApiClientFactory;
import com.daml.EOS.client.EosApiRestClient;
import com.daml.EOS.client.domain.common.transaction.PackedTransaction;
import com.daml.EOS.client.domain.common.transaction.SignedPackedTransaction;
import com.daml.EOS.client.domain.common.transaction.TransactionAction;
import com.daml.EOS.client.domain.common.transaction.TransactionAuthorization;
import com.daml.EOS.client.domain.response.chain.AbiJsonToBin;
import com.daml.EOS.client.domain.response.chain.Block;
import com.daml.EOS.client.domain.response.chain.ChainInfo;
import com.daml.EOS.client.domain.response.chain.transaction.PushedTransaction;
import com.daml.Fabric.FabricContextConfigYaml;
import com.daml.Fabric.FabricContextException;
import org.apache.commons.codec.binary.Base64;
import org.hyperledger.fabric.protos.peer.ProposalResponsePackage;
import org.hyperledger.fabric.sdk.*;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;


import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

public final class EOSContext {

    private TransactionRequest.Type ccMetaType;
    private FabricContextConfigYaml config;
    private boolean fabricTimeLogging = true;

    private String walletBaseUrl= "http://127.0.0.1:8899";
    private String chainBaseUrl = "http://127.0.0.1:8888";
    private String historyBaseUrl = "http://127.0.0.1:8888";

    public EOSContext() {
        try {
            /* Starts network configurations */
            // initNetworkConfiguration();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This method executes EOS v2.0 network configuration
     *
     * @throws Exception
     */
    public void initNetworkConfiguration() {
    }

    // Chaincode query methods
    public byte[] queryChaincode(String fcn) {
        return queryChaincode(fcn, new String[]{});
    }

    public byte[] queryChaincode(String fcn, String... args) {
        return queryChaincode(fcn, convertChaincodeArgs(args));
    }

    public byte[] queryChaincode(String fcn, byte[]... args) {
        try {
            byte[] result = null;

            String s = null;
            if (fcn == "RecordTimeRead") {
                s = "DRecordTime";
            } else if (fcn == "LedgerIDRead") {
                s = "DLedgerID";
            } else {
                String hexadecimal = byteArrayToHex(args).trim();
                System.out.println("#### hexadecimal: " + hexadecimal);

                BigInteger bigint = new BigInteger(hexadecimal, 16);

                StringBuilder sb = new StringBuilder();
                byte[] ba = Base64.encodeInteger(bigint);
                for (byte b : ba) {
                    sb.append((char)b);
                }

                if (fcn == "RawRead") {
                    s = "DState:" + sb.toString();
                } else if (fcn == "PackageRead") {
                    s = "DPackages:" + sb.toString();
                }
            }
            System.out.println("#### key: : " + s);

            String hash = sha256(s);
            System.out.println("#### sha256: " + hash);

            String rpc_url = "http://127.0.0.1:8888/v1/chain/get_table_rows";
            String payload =
                    //"{\"json\": true,\"code\": \"hello\",\"table\": \"raw\",\"scope\": \"hello\"}";
                    "{\"json\": true,\"code\": \"hello\",\"table\": \"daml\",\"scope\": \"hello\",\"index_position\": \"secondary\",\"key_type\": \"sha256\",\"upper_bound\": \"" + hash + "\",\"lower_bound\": \"" + hash + "\"}";
            System.out.println("#### payload: " + payload);

            try {
                URL url = new URL(rpc_url);
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setRequestMethod("POST");
                con.setRequestProperty("Content-Type","application/json");
                con.setDoOutput(true);
                DataOutputStream wr = new DataOutputStream(con.getOutputStream());
                wr.writeBytes(payload);
                wr.flush();
                wr.close();

                int responseCode = con.getResponseCode();
                System.out.println("#### response code : " + responseCode);

                BufferedReader iny = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String output;
                StringBuffer response = new StringBuffer();

                while ((output = iny.readLine()) != null) {
                    response.append(output);
                }
                iny.close();

                //printing result from response
                System.out.println("#### response data: " + response.toString());

                JSONParser parser = new JSONParser();
                Object obj = parser.parse( response.toString() );
                JSONObject jsonObj = (JSONObject) obj;

                JSONArray rows = (JSONArray) jsonObj.get("rows");
                if (rows.size() != 0) {
                    JSONObject row = (JSONObject) rows.get(0);
                    String value = (String) row.get("value");
                    //printing value from row
                    System.out.println("#### value: " + value);

                    result = Base64.decodeBase64(value);
                }

            } catch(Exception e) {
                e.printStackTrace();
            }

            return result;

        } catch (Throwable t) {
            if (RuntimeException.class.isAssignableFrom(t.getClass())) {
                throw (RuntimeException)t;
            } else {
                throw new FabricContextException(t);
            }
        }
    }


    //Chaincode invoke methods
    public byte[] invokeChaincode(String fcn) {
        return invokeChaincode(fcn, new String[]{});
    }

    public byte[] invokeChaincode(String fcn, String... args) {
        return invokeChaincode(fcn, convertChaincodeArgs(args));
    }

    public byte[] invokeChaincode(String fcn, byte[]... args) {
        byte[] result = null;

        try {
            /* Create the rest client */
            EosApiRestClient eosApiRestClient = EosApiClientFactory.newInstance(
                    walletBaseUrl, chainBaseUrl, historyBaseUrl).newRestClient();

            /* Get the head block */
            ChainInfo chainInfo = eosApiRestClient.getChainInfo();
            Block block = eosApiRestClient.getBlock(eosApiRestClient.getChainInfo().getHeadBlockId());

            System.out.println("#### EOSContext invokeChaincode");
            System.out.println("#### blockNum: " + block.getBlockNum().toString());
            System.out.println("#### fcn: " + fcn);
            System.out.println("#### args: " + byteArrayToHex(args).trim());

            /* Create Transaction Action Authorization */
            TransactionAuthorization transactionAuthorization = new TransactionAuthorization();
            transactionAuthorization.setActor("hello");
            transactionAuthorization.setPermission("active");

            /* Create the json array of arguments */
            Map<String, byte[][]> map = new HashMap<>(4);
            map.put("rawArgs", args);
            AbiJsonToBin data = eosApiRestClient.abiJsonToBin("hello", fcn, map);

            /* Create Transaction Action */
            TransactionAction transactionAction = new TransactionAction();
            transactionAction.setAccount("hello");
            transactionAction.setName(fcn);
            transactionAction.setData(data.getBinargs());
            transactionAction.setAuthorization(Collections.singletonList(transactionAuthorization));

            String expiration = ZonedDateTime.now(ZoneId.of("GMT")).plusMinutes(3).truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

            /* Create a transaction */
            PackedTransaction packedTransaction = new PackedTransaction();
            packedTransaction.setExpiration(expiration);
            packedTransaction.setRefBlockNum(block.getBlockNum().toString());
            packedTransaction.setRefBlockPrefix(block.getRefBlockPrefix().toString());
            packedTransaction.setMax_net_usage_words("0");
            packedTransaction.setRegion("0");
            packedTransaction.setActions(Collections.singletonList(transactionAction));

            /* Sign the Transaction */
            SignedPackedTransaction signedPackedTransaction = eosApiRestClient.signTransaction(
                    packedTransaction, Collections.singletonList("EOS6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5GDW5CV"), chainInfo.getChainId());

            /* Push the transaction */
            PushedTransaction pushedTransaction = eosApiRestClient.pushTransaction("none", signedPackedTransaction);
            System.out.println(pushedTransaction.toString());




        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;
    }

    public String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder();
        for(final byte b: a)
            sb.append(String.format("%02x", b&0xff));
        return sb.toString();
    }

    public String byteArrayToHex(byte[][] a) {
        StringBuilder sb = new StringBuilder();
        for (final byte[] b : a) {
            for(final byte c: b)
                sb.append(String.format("%02x", c&0xff));
        }
        return sb.toString();
    }

    public String sha256(String msg) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(msg.getBytes());

        return byteToHex(md.digest());
    }

    public String byteToHex(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (byte b : bytes) {
            builder.append(String.format("%02x", b));
        }
        return builder.toString();
    }

    private String makeErrorFromProposalResponse(ProposalResponse rsp) {
        ProposalResponsePackage.ProposalResponse rsp2 = rsp.getProposalResponse();
        if (rsp2 != null) {
            return rsp2.toString();
        }

        int status = rsp.getStatus().getStatus();
        String message = rsp.getMessage();
        return String.format("Chaincode returned status %d (%s)", status, message);
    }

    private byte[][] convertChaincodeArgs(String[] args) {
        byte[][] byteArgs = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            byteArgs[i] = args[i].getBytes(StandardCharsets.UTF_8);
        }
        return byteArgs;
    }

}