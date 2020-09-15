// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0



package com.daml;

import com.daml.EOS.EOSContext;
import com.daml.Fabric.FabricContext;
import com.daml.Fabric.FabricContextConfigYaml;
import org.hyperledger.fabric.sdk.BlockEvent;
import org.hyperledger.fabric.sdk.BlockListener;
import org.hyperledger.fabric.sdk.Channel;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class DAMLKVConnector {

    public String DEFAULT_LEDGER_ID = "fabric-ledger-server";

    private FabricContext ctx;
    private EOSContext etx;
    
    private static DAMLKVConnector instance;
    public static synchronized DAMLKVConnector get(boolean doEnsure, boolean doExplorer) {
        if (instance == null) {
            instance = new DAMLKVConnector(doEnsure, doExplorer);
        }
        
        return instance;
    }

    public static synchronized DAMLKVConnector get() {
        return get(false, false);
    }

    private static void logTime(String f, long time) {
        //System.out.format("%s : took %dms%n", f, System.currentTimeMillis()-time);
        //System.out.flush();
    }
    
    private String keyToString(byte[] key) {
        return Base64.getEncoder().encodeToString(key);
    }

    private boolean haveBlocks;
    private class DAMLBlockListener implements BlockListener {

        @Override
        public void received(BlockEvent blockEvent) {
            synchronized (DAMLKVConnector.this) {
                haveBlocks = true;
            }
        }
    }

    public boolean checkNewBlocks() {
        boolean rNewBlocks;
        synchronized (this) {
            rNewBlocks = haveBlocks;
            haveBlocks = false;
        }
        return rNewBlocks;
    }

    private DAMLKVConnector(boolean doEnsure, boolean doExplorer) {
        synchronized (this) {
            haveBlocks = true;
        }

        ctx = new FabricContext(doEnsure);
        etx = new EOSContext();
        if (doExplorer) ExplorerService.Run(ctx);

        // run block checker
        Channel c = ctx.getChannel();
        try {
            c.registerBlockListener(new DAMLBlockListener());
        } catch (Throwable t) {
            if (RuntimeException.class.isAssignableFrom(t.getClass())) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(t);
            }
        }

        if (doEnsure) {
            String id = getLedgerId();
            if (id.isEmpty()) {
                putLedgerId(getLocalLedgerId());
                putEOSLedgerId(getLocalLedgerId());
            }
        }
    }

    @FunctionalInterface
    private interface RetryFunction {

        byte[] call();

    }

    // if we have a chaincode invoke exception with invalid transaction - status 10 or 11, this means
    //   that there was a state read/write conflict at the Orderer. we need to retry the transaction
    private boolean checkException(String msg) {
        return (msg != null && (msg.contains("status 11") || msg.contains("status 10")));
    }

    private byte[] retryInvoke(RetryFunction f) {
        // logically, ce cannot be uninitialized here, but Java does not recognize this
        RuntimeException ce = null;

        for (int i = 0; i < 5; i++) {
            try {
                return f.call();
            } catch (RuntimeException e) {

                String msg = e.getMessage();
                // Status 11 means there was some kind of concurrency conflict at the orderer
                if (checkException(msg)) {
                    ce = e;
                    try {
                        Thread.sleep(250);
                        System.out.format("INTERNAL: retrying Fabric transaction...%n");
                    } catch (InterruptedException ie) {
                        throw new RuntimeException(ie);
                    }
                    continue;
                }

                e.printStackTrace(System.err);
                throw e;

            }
        }

        throw ce;
    }
    
    public void putValue(byte[] key, byte[] value) {
        System.out.println("========== putValue start ==========");
        long init = System.currentTimeMillis();
        retryInvoke(() -> ctx.invokeChaincode("RawWrite", key, gzipBytes(value)));
        logTime("putValue", init);
        System.out.println("========== putValue end ==========");

        putEOSValue(key, value);
    }

    public void putEOSValue(byte[] key, byte[] value) {
        System.out.println("========== putEOSValue start ==========");
        long init = System.currentTimeMillis();
        retryInvoke(() -> etx.invokeChaincode("rawwrite", key, gzipBytes(value)));
        logTime("putValue", init);
        System.out.println("========== putEOSValue end ==========");
    }

    public void putBatchAndCommit(byte[][] stateBatch) {
        System.out.println("========== putBatchAndCommit start ==========");
        long init = System.currentTimeMillis();
        retryInvoke(() -> ctx.invokeChaincode("RawBatchWrite", stateBatch));
        logTime("putBatch", init);
        System.out.println("========== putBatchAndCommit end ==========");
    }

    public void putEOSBatchAndCommit(byte[][] stateBatch) {
        System.out.println("========== putEOSBatchAndCommit start ========== ");
        long init = System.currentTimeMillis();
        retryInvoke(() -> etx.invokeChaincode("rawbatchwrite", stateBatch));
        logTime("putEOSBatch", init);
        System.out.println("========== putEOSBatchAndCommit end ==========");
    }

    public byte[] getValue(byte[] key) {
        System.out.println("========== getValue start ==========");
        byte[] data = ctx.queryChaincode("RawRead", new byte[][]{ key });
        if (data != null && data.length > 0) {

            String hexadecimal_key = etx.byteArrayToHex(key).trim();
            String hexadecimal_data = etx.byteArrayToHex(data).trim();

            BigInteger bigint_key = new BigInteger(hexadecimal_key, 16);
            BigInteger bigint_data = new BigInteger(hexadecimal_data, 16);

            StringBuilder sb_key = new StringBuilder();
            byte[] ba_key = org.apache.commons.codec.binary.Base64.encodeInteger(bigint_key);
            for (byte b : ba_key) {
                sb_key.append((char)b);
            }
            StringBuilder sb_data = new StringBuilder();
            byte[] ba_data = org.apache.commons.codec.binary.Base64.encodeInteger(bigint_data);
            for (byte b : ba_data) {
                sb_data.append((char)b);
            }

            System.out.println("#### getValue key (base64): " + "DState:" + sb_key.toString());
            System.out.println("#### getValue value (base64): " + sb_data.toString());

            data = gunzipBytes(data);
        }
        if (data.length == 0)
            return null;

        System.out.println("========== getValue end ==========");
        return data;
    }

    public byte[] getEOSValue(byte[] key) {
        System.out.println("========== getEOSValue start ==========");
        byte[] data = etx.queryChaincode("RawRead", new byte[][]{ key });
        if (data != null && data.length > 0) {

            String hexadecimal_key = etx.byteArrayToHex(key).trim();
            String hexadecimal_data = etx.byteArrayToHex(data).trim();

            BigInteger bigint_key = new BigInteger(hexadecimal_key, 16);
            BigInteger bigint_data = new BigInteger(hexadecimal_data, 16);

            StringBuilder sb_key = new StringBuilder();
            byte[] ba_key = org.apache.commons.codec.binary.Base64.encodeInteger(bigint_key);
            for (byte b : ba_key) {
                sb_key.append((char)b);
            }
            StringBuilder sb_data = new StringBuilder();
            byte[] ba_data = org.apache.commons.codec.binary.Base64.encodeInteger(bigint_data);
            for (byte b : ba_data) {
                sb_data.append((char)b);
            }

            System.out.println("#### getValue key (base64): " + "DState:" + sb_key.toString());
            System.out.println("#### getValue value (base64): " + sb_data.toString());

            data = gunzipBytes(data);
        }

        System.out.println("========== getEOSValue end ==========");
        return data;
    }

    public int putCommit(byte[] commit) {
        long init = System.currentTimeMillis();
        logTime("putCommit", init);
        byte[] newIndexBytes = retryInvoke(() -> ctx.invokeChaincode("WriteCommitLog", new byte[][]{ gzipBytes(commit) }));
        int newIndex = ByteBuffer.wrap(newIndexBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return newIndex;
    }
    
    public int getCommitHeight() {

        long init = System.currentTimeMillis();
        byte[] indexBytes = ctx.queryChaincode("ReadCommitHeight");
        logTime("getCommitHeight", init);
        if (indexBytes == null || indexBytes.length == 0)
            return 0;
        int index = ByteBuffer.wrap(indexBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
        return index;

    }

    public byte[] getCommit(int index) {
        long init = System.currentTimeMillis();
        byte[] data = ctx.queryChaincode("ReadCommit", Integer.toString(index));
        if (data != null && data.length > 0) {
            data = gunzipBytes(data);
        }
        logTime("getCommit", init);
        if (data.length == 0)
            return null;
        return data;
    }
    
    public byte[] gzipBytes(byte[] data) {
        
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            GZIPOutputStream out = new GZIPOutputStream(baos);
            out.write(data);
            out.close();
            return baos.toByteArray();
        } catch (Throwable t) {
            if (RuntimeException.class.isAssignableFrom(t.getClass())) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(t);
            }
        }
        
    }
    
    private byte[] gunzipBytes(byte[] data) {
        
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            GZIPInputStream in = new GZIPInputStream(bais);
            byte[] b = new byte[1024];
            int len;            
            while ((len = in.read(b)) != -1) {
                baos.write(b, 0, len);
            }
            in.close();
            bais.close();
            return baos.toByteArray();
        } catch (Throwable t) {
            if (RuntimeException.class.isAssignableFrom(t.getClass())) {
                throw (RuntimeException) t;
            } else {
                throw new RuntimeException(t);
            }
        }
        
    }
    
    private void putPackage(String cacheKey, byte[] value, boolean cacheOnly) {
        System.out.println("========== putPackage start ==========");
        long init = System.currentTimeMillis();
        retryInvoke(() -> ctx.invokeChaincode("PackageWrite", cacheKey.getBytes(StandardCharsets.UTF_8), gzipBytes(value)));
        logTime("putPackage", init);
        System.out.println("========== putPackage end ==========");
    }

    private void putEOSPackage(String cacheKey, byte[] value, boolean cacheOnly) {
        System.out.println("========== putEOSPackage start ==========");
        long init = System.currentTimeMillis();
        retryInvoke(() -> etx.invokeChaincode("packagewrite", cacheKey.getBytes(StandardCharsets.UTF_8), gzipBytes(value)));
        logTime("putEOSPackage", init);
        System.out.println("========== putEOSPackage end ==========");
    }

    public void putPackage(String cacheKey, byte[] value) {
        putPackage(cacheKey, value, false);
    }

    public void putEOSPackage(String cacheKey, byte[] value) {
        putEOSPackage(cacheKey, value, false);
    }
    
    public byte[] getPackage(String cacheKey) {
        System.out.println("========== getPackage start ==========");
        long init = System.currentTimeMillis();
        byte[] data = ctx.queryChaincode("PackageRead", new byte[][] { cacheKey.getBytes(StandardCharsets.UTF_8) });
        if (data != null && data.length > 0) {
            data = gunzipBytes(data);
        }
        logTime("getPackage", init);
        if (data.length == 0)
            return null;
        System.out.println("========== getPackage end ==========");
        return data;        
    }

    public byte[] getEOSPackage(String cacheKey) {
        System.out.println("========== getEOSPackage start ==========");
        long init = System.currentTimeMillis();
        byte[] data = etx.queryChaincode("PackageRead", new byte[][] { cacheKey.getBytes(StandardCharsets.UTF_8) });
        if (data != null && data.length > 0) {
            data = gunzipBytes(data);
        }
        logTime("getEOSPackage", init);
        if (data.length == 0)
            return null;
        System.out.println("========== getEOSPackage end ==========");
        return data;
    }
    
    private byte[] getPackageListBytes() {
        System.out.println("========== getPackageListBytes start ==========");
        long init = System.currentTimeMillis();
        byte[] data = ctx.queryChaincode("PackageListRead");
        logTime("getPackageListBytes", init);
        System.out.println("========== getPackageListBytes end ==========");
        return data;
    }
    
    public String[] getPackageList() {
        System.out.println("========== getPackageList start ==========");
        long init = System.currentTimeMillis();
        byte[] data = getPackageListBytes();
        ByteBuffer dataView = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        int packagesCount = dataView.getInt(0);
        String[] packages = new String[packagesCount];
        int offset = 4;
        for (int i = 0; i < packagesCount; i++) {
            int packageLen = dataView.getInt(offset);
            byte[] packageBytes = new byte[packageLen];
            for (int j = 0; j < packageLen; j++) {
                packageBytes[j] = data[offset+4+j];
            }
            packages[i] = new String(packageBytes, StandardCharsets.UTF_8);
            offset += 4 + packageLen;
        }
        logTime("getPackageList", init);
        System.out.println("========== getPackageList end ==========");
        return packages;
    }

    void putRecordTime(String time) {
        System.out.println("========== putRecordTime start ==========");
        long init = System.currentTimeMillis();
        retryInvoke(() -> ctx.invokeChaincode("RecordTimeWrite", time));
        logTime("putRecordTime", init);
        System.out.println("========== putRecordTime end ==========");
    }

    void putEOSRecordTime(String time) {
        System.out.println("========== putEOSRecordTime start ==========");
        long init = System.currentTimeMillis();
        retryInvoke(() -> etx.invokeChaincode("recordtwrite", time));
        logTime("putEOSRecordTime", init);
        System.out.println("========== putEOSRecordTime end ==========");
    }

    public String getRecordTime() {
        System.out.println("========== getRecordTime start ==========");
        long init = System.currentTimeMillis();
        byte[] bytes = ctx.queryChaincode("RecordTimeRead");
        logTime("getRecordTime", init);
        System.out.println("========== getRecordTime end ==========");
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String getEOSRecordTime() {
        System.out.println("========== getEOSRecordTime start ==========");
        long init = System.currentTimeMillis();
        byte[] bytes = etx.queryChaincode("RecordTimeRead");
        logTime("getEOSRecordTime", init);
        System.out.println("========== getEOSRecordTime end ==========");
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String getLocalLedgerId() {
        System.out.println("========== getLocalLedgerId start ==========");
        if (ctx == null) return "";
        FabricContextConfigYaml ctxConfig = ctx.getConfig();
        if (ctxConfig == null) return "";
        if (ctxConfig.ledgerId == null) return DEFAULT_LEDGER_ID;
        System.out.println("========== getLocalLedgerId end ==========");
        return ctxConfig.ledgerId;
    }

    void putLedgerId(String ledgerId) {
        System.out.println("========== putLedgerId start ==========");
        long init = System.currentTimeMillis();
        retryInvoke(() -> ctx.invokeChaincode("LedgerIDWrite", ledgerId));
        logTime("putLedgerId", init);
        System.out.println("========== putLedgerId end ==========");
    }

    void putEOSLedgerId(String ledgerId) {
        System.out.println("========== putEOSLedgerId start ==========");
        long init = System.currentTimeMillis();
        retryInvoke(() -> etx.invokeChaincode("ledgeridwrite", ledgerId));
        logTime("putEOSLedgerId", init);
        System.out.println("========== putEOSLedgerId end ==========");
    }

    public String getLedgerId() {
        System.out.println("========== getLedgerId start ==========");
        long init = System.currentTimeMillis();
        byte[] bytes = ctx.queryChaincode("LedgerIDRead");
        logTime("getLedgerId", init);
        System.out.println("========== getLedgerId end ==========");

        getEOSLedgerId();

        return new String(bytes, StandardCharsets.UTF_8);
    }

    public String getEOSLedgerId() {
        System.out.println("========== getEOSLedgerId start ==========");
        long init = System.currentTimeMillis();
        byte[] bytes = etx.queryChaincode("LedgerIDRead");
        logTime("getLedgerId", init);
        System.out.println("========== getEOSLedgerId end ==========");
        if (bytes != null)
            return new String(bytes, StandardCharsets.UTF_8);
        else
            return null;
    }
    
    public void shutdown() {

    }
    
}

