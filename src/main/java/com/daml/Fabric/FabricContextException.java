// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0



package com.daml.Fabric;

public class FabricContextException extends RuntimeException {
    
    public FabricContextException(Throwable cause) {
        super(cause);
    }
    
    public FabricContextException(String msg) {
        super(msg);
    }
    
}

