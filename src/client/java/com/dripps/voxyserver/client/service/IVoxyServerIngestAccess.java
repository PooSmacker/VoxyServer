package com.dripps.voxyserver.client.service;

public interface IVoxyServerIngestAccess {
    RemoteIngestService voxyserver$getRemoteIngestService();
    void voxyserver$setUsingRemoteIngest(boolean remoteIngest);
}