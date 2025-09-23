/**
 *
 */
package com.ityulkanov.cons;

/**
 * Constants used in the project
 */
public class ContentInfo {
    public static final String INFO_READ_PUBSUB_EVENTS = "Read PubSub Events";
    public static final String INFO_DECODE_TO_LQO_STRING = "Decode to json  string";
    public static final String INFO_DECODE_TO_JSON_STRING = "Decode to json string";
    public static final String INFO_MAP_TO_ARCHIVE = "Map to Archive";
    public static final String INFO_WRITE_AVRO_FILES = "Write File(s)";
    public static final String INFO_WINDOW = " Window";
    public static final String TAG_LEAD = "lead";
    public static final String TAG_QUOTE = "quote";
    public static final String TAG_ORDER = "transaction";
    public static final String ERR_TAG_BASE_INFO = "base_info tag not present in LQO";
    public static final String TAG_SUPPLIER_ORDER = "Supplier_order";
    public static final String ACT_RETRIEVE_JSON_CONTAINING_TYPE_OF_TRANSACTION = "Retrieve JSON object matching with type of transaction-L or Q or O or Supplier_order";
    public static final String ERR_LQOS_TRANSACTION_TYPE_NOT_PRESENT = "Transaction type lead/quote/order/Supplier_order not present in data element";
    public static final String TRANSACTION_TYPE_LQOS = "LQOS";
    public static final String ACT_CONVERT_JSON_TO_ROW = "Convert JSON to ROW";
    public static final String ACT_EXTRACT_JSON_STRING = "No Json data found while retriving";
    public static final String ACT_MSG_BASE64_DECODE_FAILED = "Base64 Decode Failed";
    public static final String TAG_BASE_INFO = "base_info";
    public static final String TAG_PUBLISH_TIME = "publish_time";
    public static final String TAG_MESSAGE_ID = "message_id";
    public static final String EXCEP_FAILED_TO_SERIALIZE = "Failed to serialize json to table row: ";
    public static final String TABLE_ID = "table_id";
    public static final String TABLE_ROW = "table_row";
    public static final String ERROR = "error";
    public static final String ERROR_TYPE = "error_type";
    public static final String ACT_BQ_INSERT = "BQ_INSERT";
    public static final String INFO_JSON_TO_TABLE_ROW = "Json To Table Row";
    public static final String INFO_WRITE_SUCCESS_REC_TO_CL = "Write Successful Records To CL";
    public static final String INFO_WRITE_FAILED_REC = "Write Failed Record";
    public static final String INFO_WRITE_JSON_TO_ERR_TABLE = "Write JSON TO TABLE Row Failed";
    public static final String INFO_CREATE_LQO_FAILED_RECORD = "Create LQO Failed Record";
    public static final String INFO_WRITE_LQO_FAILED_RECORD = "Write LQO Failed Record";
    public static final String INFO_BQ_INSERT_ERR_EXTRACT = "BQ Insert Error Extract";
    public static final String INFO_BQ_INSERT_ERR_WRITE = "BQ Insert Error Write";
    public static final String INFO_CREATE_FAILED_RECORD = "Create Validation Failed Record";
    public static final String INFO_WRITE_FAILED_RECORD = "Write Validation  Failed Record";
    public static final String DESCRIPTION_READ_FROM_PUBSUB_TOPIC = "The Cloud Pub/Sub topic to read from.";
    public static final String DESCRIPTION_DIRECTORY_TO_OUTPUT_FILES = "The directory to output files to. Must end with a slash.";
    public static final String DESCRIPTION_PREFIX_OF_FILES_TO_WRITE_TO = "The filename prefix of the files to write to.";
    public static final String DESCRIPTION_SUFFIX_OF_FILES_TO_WRITE_TO = "The suffix of the files to write.";
    public static final String DESCRIPTION_OUTPUT_SHARD_TEMPLATE = "The shard template of the output file. Specified as repeating sequences "
            + "of the letters 'S' or 'N' (example: SSS-NNN). These are replaced with the "
            + "shard number, or number of shards respectively";
    public static final String DESCRIPTION_WINDOW_DURATION = "5m";
    public static final String DESCRIPTION_AVRO_WRITE_TEMP_DIRECTORY = "The Avro Write Temporary Directory. Must end with /";
    public static final String DESCRIPTION_DATASET_TO_WRITE_CL_OUTPOUT_TO = "Dataset to write the Cleansed Layer output to";
    public static final String DESCRIPTION_TBL_SPEC_TO_WRITE_CL_ERR_OUTPUT = "Table spec to write the Cleansed Layer Err output to";
    public static final String DESCRIPTION_MAX_NUM_OUTPUT_SHARDS = "The maximum number of output shards produced when writing.";
    public static final String INFO_ERR_TAB_INSERT_FAILED = "Unable to insert some error message in error table with err";

    public static final String SEP_COLON = ":";
    public static final String SEP_DOT = ".";
    public static final String TYPE_CAST_MSG = "Type cast failed for:  ";
    public static final String TYPE_CAST_ERR = "Type cast Error";
    public static final String SCHEMA_MISS_MATCH = "Schema Mismatch";
    public static final String SRC_SYSTEM_CD = "source_system_code";
    public static final String EVENT_DATE_TIME = "event_date_time";
    public static final String PRC_DATE_TIME = "processing_date_time";
    public static final String ERR_TIMESTMP = "err_timestamp";
    public static final String AVRO_PTRANSFORM = "Avro PTransform";
    public static final String BQ_INSERT_TRANSFORM = "BQ insert PTransform";

    public static final String FILEPREFIX = "/windowed-file";
    public static final String TAG_ING_METHOD = "ingestion_method";

    public static final String FIELD_CODE = "code";
    public static final String FIELD_LIFECYCLE_STATUS_NAME = "lifecycle_status_name";
    public static final String SEP_UNDERSCORE = "_";
    public static final String FIELD_STATUS = "status";
    public static final String FIELD_CATEGORY = "category";

    public static final String INFO_CONVRT_LQO_FIELDS_TRUSTED = "Convert common fields in L,Q,O for Trusted layer";
    public static final String INFO_CONVRT_QO_FIELDS_TRUSTED = "Convert common fields in Q,O for Trusted layer";
    public static final String INFO_CONVRT_QUOTE_FIELDS_TRUSTED = "Action on Quote Specific Tables for Trusted layer";
    public static final String INFO_INSERT_TO_TRUSTED_QUOTE_DISCOUNTS_APPLED = "Insert to BQ Trusted - Quote Discounts_Applied";
    public static final String INFO_INSERT_TO_TRUSTED_QUOTE_LINE_ITEMS = "Insert to BQ Trusted - Quote Line_Items";
    public static final String INFO_INSERT_TO_TRUSTED_QUOTE = "Insert to BQ Trusted - Quote";


    public static final String DESCRIPTION_DATA_SET_TRUSTED = "Trusted DataSet ";

    public static final String DESCRIPTION_TRUSTED_ERR_TBL = "Trusted DataSet Error Table";

    public static final String BQ_DATE_FORMAT = "yyyy-MM-dd";

    public static final String BQ_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String SYS_GEN_DEFAULT_YEAR = "1970";
    public static final String FORCED_DEFAULT_YEAR = "9999";
    public static final String DESCRIPTION_DATASET_TACTICAL = "Tactical DataSet ";
    public static final String DESCRIPTION_TACTICAL_ERR_TBL = "Tactical DataSet Error Table";

    public static final String TBL_FIELD_SRC_SYS_CODE = "source_system_code";
    public static final String TBL_FIELD_SRC_SYS_NAME = "source_system_name";
    public static final String TBL_FIELD_JDA_SOURCING_ROUTE = "JDA_sourcing_route";
    public static final String TBL_FIELD_SOURCING_ROUTE = "sourcing_route";
    public static final String INFO_SUPPLIER_TRANSFORM = "Convert required fields in Supplier for Trusted layer";
    public static final String INFO_INSERT_TO_TRUSTED_SUPPLIER = "Insert to BQ Trusted - Supplier";
    public static final String INFO_INSERT_TO_TRUSTED_LEGAL_ENTITY = "Insert to BQ Trusted - Supplier info for Legal Entity";

    public static final String INFO_SKU_TRANSFORM = "Transaformation for required fields in Stock Keeping Unit in Trusted layer";
    public static final String TBL_FIELD_JDA_SUPP_CODE = "jda_supplier_code";
    public static final String TBL_FIELD_SUPP_CODE = "supplier_code";
    public static final String INFO_SUPPLIER_COST_PRICE_RELATED_TRANSFOR_IN_TRSTED_LYR_FAILED = "Supplier Cost Price related Transformation in Trusted Layer failed";
    public static final String INFO_STOCK_KEEPING_UNIT_INSERT = "Stock Keeping Unit insert";
    public static final String TBL_FIELD_PRODUCTS = "products";
    public static final String INFO_STOCK_KEEPING_UNIT_SERVICE_TRUSTED_PTRANSFORM = "Stock Keeping Unit Service Trusted PTransform";

    public static final String INFO_CORE_SALES_SERVICE_TRUSTED_PTRANSFORM = "Core Sales Service Trusted PTransform";

    public static final String INFO_REPLENISH_ORDR_SERVICE_TRUSTED_PTRANSFORM = "Replenishment Order Service Trusted PTransform";
    public static final String INFO_REPLENISH_ORDR_TRANSFORM = "Convert required fields in Replenishment Order for Trusted layer";
    public static final String INFO_REPLENISH_PTRANSFORM_FAILED = "Replenishment Transformation failed";
    public static final String INFO_BQ_TRSTD_RCRD_INSRT = "BQ  trusted record insert ";


    public static final String INFO_SUPPLIER_ORDER_LQO_TRUSTED_PTRANSFORM = "Supplier Order for LQO Service Trusted PTransform";
    public static final String DESCRIPTION_READ_FROM_PUBSUB_SUBSCRIPTION = "The Cloud Pub/Sub subscrition to read from.";

    public static final String INFO_HYBRIS_FULFILMENT_TRUSTED_PTRANSFORM = "Hybris Fulfilment Service Trusted PTransform";
    public static final String INFO_HYBRIS_FULFILMENT_COMMON_TRANSFORM = "Hybris Fulfilment Service Trusted PTransform";
    public static final String TBL_FIELD_CUSTOMER = "customer";
    public static final String TBL_FIELD_PRODUCTCODE = "productCode";
    public static final String TBL_FIELD_SKU = "SKU";
    public static final String TBL_FIELD_CLICKNCOLLECT = "clickNCollectLineItem";
    public static final String TBL_FIELD_CONSIGNMENTS = "consignments";
    public static final String TBL_FIELD_PH_NUMBERS = "phoneNumbers";
    public static final String TBL_FIELD_PH = "Phone";
    public static final String TBL_FIELD_NUMBER = "number";
    public static final String TBL_FIELD_TYPE_NAME = "type_name";
    public static final String TBL_FIELD_CONSIGNMENTS_NUM = "Consignments_Number";
    public static final String TBL_FIELD_ORDER = "order";
    public static final String TBL_FIELD_CUSTOMERCODE = "Customer_Code";

    public static final String TBL_FIELD_CANCELLEDFLAG = "cancelledFlag";
    public static final String TBL_FIELD_SOURCE_DESC = "Source_Desc";
    public static final String TBL_FIELD_SOURCE_IREPLEN = "iReplen";
    public static final String TBL_FIELD_SOURCE_DCS = "DCS";

    public static final String INFO_INSTALLATION_ORDER_SERVICE_TRUSTED_PTRANSFORM = "Installation Order Service Trusted PTransform";
    public static final String INFO_STOCK_ADJ_SERVICE_TRUSTED_PTRANSFORM = "Stock Adjustment Service Trusted PTransform";

    public static final String INFO_STOCK_SERVICE_TRUSTED_PTRANSFORM = "Stock Service Trusted PTransform";

    public static final String INFO_FLEXIPOD_SERVICE_RUN_DAG = "Flexipod Service Run Data Updating DAG";

    public static final String BQ_TRUSTED_RECORD_INSERT = "BQ trusted record insert";
    public static final String INFO_IBT_SERVICE_TRUSTED_PTRANSFORM = "IBT Service Trusted PTransform";
}
