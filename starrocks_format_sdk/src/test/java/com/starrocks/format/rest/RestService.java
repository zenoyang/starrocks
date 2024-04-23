package com.starrocks.format.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.starrocks.format.rest.models.Schema;
import com.starrocks.format.rest.models.TabletCommitInfo;
import com.starrocks.format.rest.models.TabletFailInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Function;

public class RestService {

    private static final Logger LOG = LogManager.getLogger(RestService.class);

    private static final String STARROCKS_REQUEST_RETRIES = "starrocks.request.retries";
    private static final String STARROCKS_REQUEST_CONNECT_TIMEOUT_MS = "starrocks.request.connect.timeout.ms";
    private static final String STARROCKS_REQUEST_READ_TIMEOUT_MS = "starrocks.request.read.timeout.ms";
    private static final int STARROCKS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT = 30 * 1000;
    private static final int STARROCKS_REQUEST_READ_TIMEOUT_MS_DEFAULT = 30 * 1000;

    private static final String STARROCKS_PASSWORD = "";
    private static final String STARROCKS_USER = "root";

    public final static int REST_RESPONSE_STATUS_OK = 200;
    private final static String STARROCKS_ENDPOINTS = "http://127.0.0.1:8030/";
    private static final String API_PREFIX = "api";
    private static final String SCHEMA = "_schema";
    private static final String QUERY_PLAN = "_query_plan";

    public static final String DB = "db";
    public static final String TABLE = "table";
    public static final String LABEL = "label";

    // Transaction
    public static final String TIMEOUT_KEY = "timeout";
    public static final String TXN_ID = "txn_id";

    private static final String SOURCE_TYPE = "source_type";

    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    /**
     * send request to StarRocks FE and get response json string.
     *
     * @param cfg     configuration of request
     * @param request {@link HttpRequestBase} real request
     * @return StarRocks FE response in json string
     * @throws RuntimeException throw when cannot connect to StarRocks FE
     */
    private static String send(Map<String, String> cfg, HttpRequestBase request) throws
            RuntimeException {
        int connectTimeout = StringUtils.isBlank(cfg.get(STARROCKS_REQUEST_CONNECT_TIMEOUT_MS)) ?
                STARROCKS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT : Integer.parseInt(cfg.get(STARROCKS_REQUEST_CONNECT_TIMEOUT_MS));
        int socketTimeout = StringUtils.isBlank(cfg.get(STARROCKS_REQUEST_READ_TIMEOUT_MS)) ?
                STARROCKS_REQUEST_READ_TIMEOUT_MS_DEFAULT : Integer.parseInt(cfg.get(STARROCKS_REQUEST_READ_TIMEOUT_MS));
        int retries =
                StringUtils.isBlank(cfg.get(STARROCKS_REQUEST_RETRIES)) ? 1 :
                        Integer.parseInt(cfg.get(STARROCKS_REQUEST_RETRIES));

        LOG.info("connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                connectTimeout, socketTimeout, retries);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .build();

        request.setConfig(requestConfig);

        UsernamePasswordCredentials creds = new UsernamePasswordCredentials(STARROCKS_USER, STARROCKS_PASSWORD);
        HttpClientContext context = HttpClientContext.create();
        try {
            request.addHeader(new BasicScheme().authenticate(creds, request, context));
            request.addHeader(DB, cfg.get(DB));
            request.addHeader(TABLE, cfg.get(TABLE));
            request.addHeader(TXN_ID, cfg.get(TXN_ID));
            request.addHeader(LABEL, cfg.get(LABEL));
        } catch (AuthenticationException e) {
            LOG.error("connect {} failed", request.getURI(), e);
            throw new RuntimeException("connection failed");
        }

        LOG.info("Send request to StarRocks FE '{}' with user '{}'.", request.getURI(), STARROCKS_USER);

        for (int attempt = 0; attempt < retries; attempt++) {
            LOG.debug("Attempt {} to request {}.", attempt, request.getURI());
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                CloseableHttpResponse response = httpClient.execute(request, context);
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != HttpStatus.SC_OK) {
                    LOG.warn("Failed to get response from StarRocks FE {}, http code is {}",
                            request.getURI(), statusCode);
                    continue;
                }
                String res = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                System.out.println("resp: " + res);
                if (res != null && res.contains("FAILED")) {
                    continue;
                }
                LOG.trace("Success get response from StarRocks FE: {}, response is: {}.",
                        request.getURI(), res);
                return res;
            } catch (IOException e) {
                throw new RuntimeException("response error, " + e.getMessage(), e);
            }
        }
        throw new RuntimeException("response error 1");
    }

    /**
     * Request to begin transaction.
     */
    public static TransactionContext beginTransaction(Map<String, String> cfg) {
        return requestTransaction(TxnOperation.TXN_BEGIN, cfg, httpPost -> httpPost);
    }

    /**
     * Request to prepare transaction.
     */
    public static TransactionContext prepareTransaction(Map<String, String> cfg,
                                                        List<TabletCommitInfo> tabletCommitInfos) {
        return requestTransaction(TxnOperation.TXN_PREPARE, cfg, httpPost -> {
            if (null == tabletCommitInfos) {
                return httpPost;
            }

            try {
                String body = JSON_OBJECT_MAPPER.writeValueAsString(new HashMap<String, Object>(1) {
                    private static final long serialVersionUID = 2004207872111967646L;

                    {
                        put("committed_tablets", tabletCommitInfos);
                    }
                });

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Request to commit transaction with body '{}'.", body);
                }
                StringEntity entity = new StringEntity(body, StandardCharsets.UTF_8);
                entity.setContentType(ContentType.APPLICATION_JSON.toString());
                httpPost.setEntity(entity);
                return httpPost;
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Write committed tablets as json error, " + e.getMessage(), e);
            }
        });
    }

    /**
     * Request to commit transaction.
     */
    public static TransactionContext commitTransaction(Map<String, String> cfg) {
        return requestTransaction(TxnOperation.TXN_COMMIT, cfg, httpPost -> httpPost);
    }

    /**
     * Request to rollback transaction.
     */
    public static TransactionContext rollbackTransaction(Map<String, String> cfg, List<TabletFailInfo> tabletFailInfos) {
        return requestTransaction(TxnOperation.TXN_ROLLBACK, cfg, httpPost -> {
            if (null == tabletFailInfos) {
                return httpPost;
            }

            try {
                String body = JSON_OBJECT_MAPPER.writeValueAsString(new HashMap<String, Object>(1) {
                    private static final long serialVersionUID = -6125397601152855117L;

                    {
                        put("failed_tablets", tabletFailInfos);
                    }
                });

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Request to rollback transaction with body '{}'.", body);
                }
                StringEntity entity = new StringEntity(body, StandardCharsets.UTF_8);
                entity.setContentType(ContentType.APPLICATION_JSON.toString());
                httpPost.setEntity(entity);
                return httpPost;
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("Write failed tablets as json error, " + e.getMessage(), e);
            }
        });
    }

    private static TransactionContext requestTransaction(TxnOperation txnOperation,
                                                         Map<String, String> cfg,
                                                         Function<HttpPost, HttpPost> httpPostFunction) {
        String txnId = cfg.getOrDefault(TXN_ID, StringUtils.EMPTY);
        LOG.info("Request to {} transaction {}", txnOperation, txnId);
        String uriStr = getTransUriStr(txnOperation);
        try {
            URIBuilder uriBuilder = new URIBuilder(uriStr);
            if (TxnOperation.TXN_BEGIN.equals(txnOperation)) {
                // FIXME maybe we should move LoadJobSourceType from fe-core to fe-common
                uriBuilder.addParameter(SOURCE_TYPE, "10");
            }
            HttpPost httpPost = httpPostFunction.apply(new HttpPost(uriBuilder.build()));
            String jsonResp = send(cfg, httpPost);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Request to {} transaction {}, and response is '{}'.", txnOperation, txnId, jsonResp);
            }
            TransactionContext txnContext = parseResp(jsonResp, TransactionContext.class);
            if (txnContext.notOk()) {
                throw new IllegalStateException(txnContext.getMessage());
            }
            return txnContext;
        } catch (URISyntaxException e) {
            throw new IllegalStateException("invalid uri: " + uriStr, e);
        }
    }

    public static Schema getShardInfo(Map<String, String> cfg) {
        LOG.trace("getShardInfo.");
        HttpGet httpGet = new HttpGet(getSchemaUriStr(SCHEMA, cfg.get("db"), cfg.get("table")));

        String entity = "{\"sql\": \" no need \"}";
        LOG.debug("get Sending to StarRocks FE is: '{}'.", entity);
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");

        String resStr = send(cfg, httpGet);
        LOG.debug("Find partition response is '{}'.", resStr);
        return parseResp(resStr, Schema.class);
    }

    static String getTransUriStr(TxnOperation txnOperation) throws IllegalArgumentException {
        return STARROCKS_ENDPOINTS + API_PREFIX + "/transaction/" + txnOperation.getValue();
    }

    static String getSchemaUriStr(String operator, String db, String tbl) throws IllegalArgumentException {
        return STARROCKS_ENDPOINTS + API_PREFIX + "/" + db + "/" + tbl + "/" + operator + "?withExtendedInfo=true";
    }

    static <T> T parseResp(String resp, Class<T> clazz) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            return mapper.readValue(resp, clazz);
        } catch (JsonParseException e) {
            System.out.println(e.getMessage());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    public enum TxnOperation {

        TXN_BEGIN("begin"),

        TXN_PREPARE("prepare"),

        TXN_COMMIT("commit"),

        TXN_ROLLBACK("rollback");

        private final String value;

        TxnOperation(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return getValue();
        }

        public String getValue() {
            return value;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TransactionContext {

        @JsonProperty("Status")
        private String status;

        @JsonProperty("Message")
        private String message;

        @JsonProperty("Label")
        private String label;

        @JsonProperty("TxnId")
        private Long txnId;

        public TransactionContext() {
        }

        public TransactionContext(String label, Long txnId) {
            this.label = label;
            this.txnId = txnId;
        }

        public boolean isOk() {
            return "OK".equalsIgnoreCase(getStatus());
        }

        public boolean notOk() {
            return !isOk();
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", "[", "]")
                    .add("status='" + status + "'")
                    .add("message='" + message + "'")
                    .add("label='" + label + "'")
                    .add("txnId=" + txnId)
                    .toString();
        }

        public String getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }

        public String getLabel() {
            return label;
        }

        public Long getTxnId() {
            return txnId;
        }

    }

}
