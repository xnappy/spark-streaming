package com.oneandone.spark.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

/**
 * created by mincekara
 */
public class BamRecord implements java.io.Serializable {
    /**
     * Start Event:
     {
        "@version":1,
        "@timestamp":"2017-07-18T12:52:17.331Z",
        "documentType":"bam_event",
        "eventType":"ACTIVITY_START",
        "technicalOrigin":"vm-mincekara-spp.sandbox.lan",
        "businessOrigin":"BO-123",
        "businessProcessId":"iptv-zattoo-gateway-v1.0",
        "businessProcessName":"iptv-zattoo-gateway-v1.0",
        "businessProcessVersion":"1.0.4",
        "businessProcessActivityId":"com.oneandone.access.services.entertainment.iptv.gateway.zattoo.webservice.delegate.AccountInfoServiceDelegate_getRegionInfo",
        "businessProcessActivityName":null,
        "businessProcessInstanceId":null,
        "payload":{
            "spp_trace_wf_id":"8bdc81ad-9733-4654-b3c9-db447b7b1b0e"
        }
     }
     * End Event:
     {
        "@version":1,
        "@timestamp":"2017-07-18T12:52:17.389Z",
        "documentType":"bam_event",
        "eventType":"ACTIVITY_END",
        "technicalOrigin":"vm-mincekara-spp.sandbox.lan",
        "businessOrigin":"BO-123",
        "businessProcessId":"iptv-zattoo-gateway-v1.0",
        "businessProcessName":"iptv-zattoo-gateway-v1.0",
        "businessProcessVersion":"1.0.4",
        "businessProcessActivityId":"com.oneandone.access.services.entertainment.iptv.gateway.zattoo.webservice.delegate.AccountInfoServiceDelegate_getRegionInfo",
        "businessProcessActivityName":null,
        "businessProcessInstanceId":null,
        "durationInMillis":60,
        "payload":{
            "ipCheckResult":"YES",
            "spp_trace_wf_id":"8bdc81ad-9733-4654-b3c9-db447b7b1b0e"
        }
     }
     */

    //BAM Event Inhalte
    @JsonProperty("@version")
    private int version;
    @JsonProperty("@timestamp")
    private Timestamp timestamp;
    private String documentType;
    private String eventType;
    private String technicalOrigin;
    private String businessOrigin;
    private String businessProcessId;
    private String businessProcessName;
    private String businessProcessVersion;
    private String businessProcessActivityId;
    private String businessProcessActivityName;
    private String businessProcessInstanceId;
    private long durationInMillis;
    //private Date timestamAsDate;
    private Map<String, String> payload;

    //Convert Timestamp
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    public static final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final SimpleDateFormat ISO_FORMATTER = new SimpleDateFormat(ISO_FORMAT);
    static {
        ISO_FORMATTER.setTimeZone(UTC);
    }

    public static String dateFormat(Timestamp timestamp) {
        return ISO_FORMATTER.format(timestamp);
    }

    // Getter/Setter
    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    /*
    public Date getTimestampAsString() {
        return timestamAsDate;
    }

    public void setTimestampAsString(String timestamAsDate) {
        try {
            Date date = ISO_FORMATTER.parse(timestamAsDate);
            this.timestamAsDate = new Date(date.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //this.timestamAsDate = timestamAsDate;
    }
    */

    public String getDocumentType() {
        return documentType;
    }

    public void setDocumentType(String documentType) {
        this.documentType = documentType;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTechnicalOrigin() {
        return technicalOrigin;
    }

    public void setTechnicalOrigin(String technicalOrigin) {
        this.technicalOrigin = technicalOrigin;
    }

    public String getBusinessOrigin() {
        return businessOrigin;
    }

    public void setBusinessOrigin(String businessOrigin) {
        this.businessOrigin = businessOrigin;
    }

    public String getBusinessProcessName() {
        return businessProcessName;
    }

    public void setBusinessProcessName(String businessProcessName) {
        this.businessProcessName = businessProcessName;
    }

    public String getBusinessProcessVersion() {
        return businessProcessVersion;
    }

    public void setBusinessProcessVersion(String businessProcessVersion) {
        this.businessProcessVersion = businessProcessVersion;
    }

    public String getBusinessProcessActivityName() {
        return businessProcessActivityName;
    }

    public void setBusinessProcessActivityName(String businessProcessActivityName) {
        this.businessProcessActivityName = businessProcessActivityName;
    }

    public String getBusinessProcessInstanceId() {
        return businessProcessInstanceId;
    }

    public void setBusinessProcessInstanceId(String businessProcessInstanceId) {
        this.businessProcessInstanceId = businessProcessInstanceId;
    }

    public long getDurationInMillis() {
        return durationInMillis;
    }

    public void setDurationInMillis(long durationInMillis) {
        this.durationInMillis = durationInMillis;
    }

    public Map<String, String> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, String> payload) {
        this.payload = payload;
    }

    public String getBusinessProcessActivityId() {
        return businessProcessActivityId;
    }

    public void setBusinessProcessActivityId(String businessProcessActivityId) {
        this.businessProcessActivityId = businessProcessActivityId;
    }

    public String getBusinessProcessId() {
        return businessProcessId;
    }

    public void setBusinessProcessId(String businessProcessId) {
        this.businessProcessId = businessProcessId;
    }
}