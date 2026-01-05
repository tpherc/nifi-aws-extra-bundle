/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package au.com.chribro.nifi.processors.aws.s3;

import com.amazonaws.AmazonClientException;
// import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
// import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
// import com.amazonaws.services.s3.model.AccessControlList;
// import com.amazonaws.services.s3.model.AmazonS3Exception;
// import com.amazonaws.services.s3.model.CannedAccessControlList;
// import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
// import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
// import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
// import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
// import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
// import com.amazonaws.services.s3.model.MultipartUpload;
// import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
// import com.amazonaws.services.s3.model.ObjectTagging;
// import com.amazonaws.services.s3.model.PartETag;
// import com.amazonaws.services.s3.model.PutObjectRequest;
// import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.StorageClass;
// import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
// import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processors.aws.s3.AbstractS3Processor;
import org.apache.nifi.processors.aws.s3.AmazonS3EncryptionService;
import org.apache.nifi.processors.aws.s3.CopyS3Object;
import org.apache.nifi.processors.aws.s3.DeleteS3Object;
import org.apache.nifi.processors.aws.s3.FetchS3Object;
import org.apache.nifi.processors.aws.s3.GetS3ObjectMetadata;
import org.apache.nifi.processors.aws.s3.GetS3ObjectTags;
import org.apache.nifi.processors.aws.s3.ListS3;
import org.apache.nifi.processors.aws.s3.PutS3Object;
import org.apache.nifi.processors.aws.s3.TagS3Object;
// import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.transfer.ResourceTransferSource;

import jakarta.xml.bind.DatatypeConverter;

// import java.io.File;
// import java.io.FileInputStream;
// import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
// import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
// import java.nio.file.Files;
// import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
// import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
// import java.util.concurrent.atomic.AtomicLong;
// import java.util.concurrent.locks.Lock;
// import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.nifi.processors.aws.util.RegionUtilV1.S3_REGION;
import static org.apache.nifi.processors.transfer.ResourceTransferProperties.FILE_RESOURCE_SERVICE;
import static org.apache.nifi.processors.transfer.ResourceTransferProperties.RESOURCE_TRANSFER_SOURCE;
import static org.apache.nifi.processors.transfer.ResourceTransferUtils.getFileResource;

@SupportsBatching
@SeeAlso({CreateS3MultipartUpload.class, CompleteS3MultipartUpload.class, PutS3Object.class, FetchS3Object.class, DeleteS3Object.class, ListS3.class, CopyS3Object.class, GetS3ObjectMetadata.class, GetS3ObjectTags.class, TagS3Object.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Amazon", "S3", "AWS", "Archive", "Upload", "Part", "Multipart"})
@CapabilityDescription("Uploads a part in a multipart upload.")
@DynamicProperty(name = "The name of a User-Defined Metadata field to add to the S3 Object",
        value = "The value of a User-Defined Metadata field to add to the S3 Object",
        description = "Allows user-defined metadata to be added to the S3 object as key/value pairs",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@ReadsAttribute(attribute = "filename", description = "Uses the FlowFile's filename as the filename for the S3 object")
@WritesAttributes({
    @WritesAttribute(attribute = "s3.url", description = "The URL that can be used to access the S3 object"),
    @WritesAttribute(attribute = "s3.bucket", description = "The S3 bucket where the Object was put in S3"),
    @WritesAttribute(attribute = "s3.key", description = "The S3 key within where the Object was put in S3"),
    @WritesAttribute(attribute = "s3.contenttype", description = "The S3 content type of the S3 Object that put in S3"),
    @WritesAttribute(attribute = "s3.version", description = "The version of the S3 Object that was put to S3"),
    @WritesAttribute(attribute = "s3.exception", description = "The class name of the exception thrown during processor execution"),
    @WritesAttribute(attribute = "s3.additionalDetails", description = "The S3 supplied detail from the failed operation"),
    @WritesAttribute(attribute = "s3.statusCode", description = "The HTTP error code (if available) from the failed operation"),
    @WritesAttribute(attribute = "s3.errorCode", description = "The S3 moniker of the failed operation"),
    @WritesAttribute(attribute = "s3.errorMessage", description = "The S3 exception message from the failed operation"),
    @WritesAttribute(attribute = "s3.etag", description = "The ETag of the S3 Object"),
    @WritesAttribute(attribute = "s3.contentdisposition", description = "The content disposition of the S3 Object that put in S3"),
    @WritesAttribute(attribute = "s3.cachecontrol", description = "The cache-control header of the S3 Object"),
    @WritesAttribute(attribute = "s3.uploadId", description = "The uploadId used to upload the Object to S3"),
    @WritesAttribute(attribute = "s3.expiration", description = "A human-readable form of the expiration date of " +
            "the S3 object, if one is set"),
    @WritesAttribute(attribute = "s3.sseAlgorithm", description = "The server side encryption algorithm of the object"),
    @WritesAttribute(attribute = "s3.usermetadata", description = "A human-readable form of the User Metadata of " +
            "the S3 object, if any was set"),
    @WritesAttribute(attribute = "s3.encryptionStrategy", description = "The name of the encryption strategy, if any was set"), })
public class UploadS3Part extends AbstractS3Processor {

    public static final long MIN_S3_PART_SIZE = 50L * 1024L * 1024L;
    public static final long MAX_S3_PUTOBJECT_SIZE = 5L * 1024L * 1024L * 1024L;
    public static final String NO_SERVER_SIDE_ENCRYPTION = "None";
    public static final String CONTENT_DISPOSITION_INLINE = "inline";
    public static final String CONTENT_DISPOSITION_ATTACHMENT = "attachment";

    private static final Set<String> STORAGE_CLASSES = Collections.unmodifiableSortedSet(new TreeSet<>(
            Arrays.stream(StorageClass.values()).map(StorageClass::name).collect(Collectors.toSet())
    ));

    public static final PropertyDescriptor UPLOAD_ID = new PropertyDescriptor.Builder()
            .name("Upload ID")
            .displayName("Upload ID")
            .description("The uploadId used to upload the Object to S3")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .defaultValue("${s3.uploadId}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PART_NUMBER = new PropertyDescriptor.Builder()
            .name("Part Number")
            .displayName("Part Number")
            .description("The S3 Multipart part number")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .defaultValue("${fragment.index}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MD5_DIGEST = new PropertyDescriptor.Builder()
        .name("MD5 Digest")
        .displayName("MD5 Digest")
        .description("The (not yet) optional, but recommended, MD5 hash of the content of this " + 
                "part. If specified, this value will be sent to Amazon S3 to " + 
                "verify the data integrity when the data reaches Amazon S3.")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
        .defaultValue("${content_md5}")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .build(); 

    public static final PropertyDescriptor EXPIRATION_RULE_ID = new PropertyDescriptor.Builder()
            .name("Expiration Time Rule")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Content Type")
            .displayName("Content Type")
            .description("Sets the Content-Type HTTP header indicating the type of content stored in the associated " +
                    "object. The value of this header is a standard MIME type.\n" +
                    "AWS S3 Java client will attempt to determine the correct content type if one hasn't been set" +
                    " yet. Users are responsible for ensuring a suitable content type is set when uploading streams. If " +
                    "no content type is provided and cannot be determined by the filename, the default content type " +
                    "\"application/octet-stream\" will be used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONTENT_DISPOSITION = new PropertyDescriptor.Builder()
            .name("Content Disposition")
            .displayName("Content Disposition")
            .description("Sets the Content-Disposition HTTP header indicating if the content is intended to be displayed inline or should be downloaded.\n " +
                    "Possible values are 'inline' or 'attachment'. If this property is not specified, object's content-disposition will be set to filename. " +
                    "When 'attachment' is selected, '; filename=' plus object key are automatically appended to form final value 'attachment; filename=\"filename.jpg\"'.")
            .required(false)
            .allowableValues(CONTENT_DISPOSITION_INLINE, CONTENT_DISPOSITION_ATTACHMENT)
            .build();

    public static final PropertyDescriptor CACHE_CONTROL = new PropertyDescriptor.Builder()
            .name("Cache Control")
            .displayName("Cache Control")
            .description("Sets the Cache-Control HTTP header indicating the caching directives of the associated object. Multiple directives are comma-separated.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STORAGE_CLASS = new PropertyDescriptor.Builder()
            .name("Storage Class")
            .required(true)
            .allowableValues(STORAGE_CLASSES)
            .defaultValue(StorageClass.Standard.name())
            .build();

    public static final PropertyDescriptor MULTIPART_THRESHOLD = new PropertyDescriptor.Builder()
            .name("Multipart Threshold")
            .description("Specifies the file size threshold for switch from the PutS3Object API to the " +
                    "PutS3MultipartUpload API.  Flow files bigger than this limit will be sent using the stateful " +
                "multipart process. The valid range is 50MB to 5GB.")
            .required(true)
            .defaultValue("5 GB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(MIN_S3_PART_SIZE, MAX_S3_PUTOBJECT_SIZE))
            .build();

    public static final PropertyDescriptor MULTIPART_PART_SIZE = new PropertyDescriptor.Builder()
            .name("Multipart Part Size")
        .description("Specifies the part size for use when the PutS3Multipart Upload API is used. " +
                    "Flow files will be broken into chunks of this size for the upload process, but the last part " +
            "sent can be smaller since it is not padded. The valid range is 50MB to 5GB.")
            .required(true)
            .defaultValue("5 GB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(MIN_S3_PART_SIZE, MAX_S3_PUTOBJECT_SIZE))
            .build();

    public static final PropertyDescriptor MULTIPART_S3_AGEOFF_INTERVAL = new PropertyDescriptor.Builder()
            .name("Multipart Upload AgeOff Interval")
            .description("Specifies the interval at which existing multipart uploads in AWS S3 will be evaluated " +
                    "for ageoff.  When processor is triggered it will initiate the ageoff evaluation if this interval has been " +
                    "exceeded.")
            .required(true)
            .defaultValue("60 min")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor MULTIPART_S3_MAX_AGE = new PropertyDescriptor.Builder()
            .name("Multipart Upload Max Age Threshold")
            .description("Specifies the maximum age for existing multipart uploads in AWS S3.  When the ageoff " +
                    "process occurs, any upload older than this threshold will be aborted.")
            .required(true)
            .defaultValue("7 days")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor SERVER_SIDE_ENCRYPTION = new PropertyDescriptor.Builder()
            .name("server-side-encryption")
            .displayName("Server Side Encryption")
            .description("Specifies the algorithm used for server side encryption.")
            .required(true)
            .allowableValues(NO_SERVER_SIDE_ENCRYPTION, ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
            .defaultValue(NO_SERVER_SIDE_ENCRYPTION)
            .build();

    public static final PropertyDescriptor OBJECT_TAGS_PREFIX = new PropertyDescriptor.Builder()
            .name("s3-object-tags-prefix")
            .displayName("Object Tags Prefix")
            .description("Specifies the prefix which would be scanned against the incoming FlowFile's attributes and the matching attribute's " +
                    "name and value would be considered as the outgoing S3 object's Tag name and Tag value respectively. For Ex: If the " +
                    "incoming FlowFile carries the attributes tagS3country, tagS3PII, the tag prefix to be specified would be 'tagS3'")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor REMOVE_TAG_PREFIX = new PropertyDescriptor.Builder()
            .name("s3-object-remove-tags-prefix")
            .displayName("Remove Tag Prefix")
            .description("If set to 'True', the value provided for '" + OBJECT_TAGS_PREFIX.getDisplayName() + "' will be removed from " +
                    "the attribute(s) and then considered as the Tag name. For ex: If the incoming FlowFile carries the attributes tagS3country, " +
                    "tagS3PII and the prefix is set to 'tagS3' then the corresponding tag values would be 'country' and 'PII'")
            .allowableValues(new AllowableValue("true", "True"), new AllowableValue("false", "False"))
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor MULTIPART_TEMP_DIR = new PropertyDescriptor.Builder()
            .name("s3-temporary-directory-multipart")
            .displayName("Temporary Directory Multipart State")
            .description("Directory in which, for multipart uploads, the processor will locally save the state tracking the upload ID and parts "
                    + "uploaded which must both be provided to complete the upload.")
            .required(true)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .defaultValue("${java.io.tmpdir}")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BUCKET_WITH_DEFAULT_VALUE,
            KEY,
            UPLOAD_ID,
            PART_NUMBER,
            MD5_DIGEST,
            S3_REGION,
            AWS_CREDENTIALS_PROVIDER_SERVICE,
            RESOURCE_TRANSFER_SOURCE,
            FILE_RESOURCE_SERVICE,
            STORAGE_CLASS,
            ENCRYPTION_SERVICE,
            SERVER_SIDE_ENCRYPTION,
            CONTENT_TYPE,
            CONTENT_DISPOSITION,
            CACHE_CONTROL,
            OBJECT_TAGS_PREFIX,
            REMOVE_TAG_PREFIX,
            TIMEOUT,
            EXPIRATION_RULE_ID,
            FULL_CONTROL_USER_LIST,
            READ_USER_LIST,
            WRITE_USER_LIST,
            READ_ACL_LIST,
            WRITE_ACL_LIST,
            OWNER,
            CANNED_ACL,
            SSL_CONTEXT_SERVICE,
            ENDPOINT_OVERRIDE,
            SIGNER_OVERRIDE,
            S3_CUSTOM_SIGNER_CLASS_NAME,
            S3_CUSTOM_SIGNER_MODULE_LOCATION,
            MULTIPART_THRESHOLD,
            MULTIPART_PART_SIZE,
            MULTIPART_S3_AGEOFF_INTERVAL,
            MULTIPART_S3_MAX_AGE,
            MULTIPART_TEMP_DIR,
            USE_CHUNKED_ENCODING,
            USE_PATH_STYLE_ACCESS,
            PROXY_CONFIGURATION_SERVICE
    );

    final static String S3_BUCKET_KEY = "s3.bucket";
    final static String S3_OBJECT_KEY = "s3.key";
    final static String S3_CONTENT_TYPE = "s3.contenttype";
    final static String S3_CONTENT_DISPOSITION = "s3.contentdisposition";
    final static String S3_UPLOAD_ID_ATTR_KEY = "s3.uploadId";
    final static String S3_PART_NUMBER_ATTR_KEY = "s3.partNumber";
    final static String S3_VERSION_ATTR_KEY = "s3.version";
    final static String S3_ETAG_ATTR_KEY = "s3.etag";
    final static String S3_CACHE_CONTROL = "s3.cachecontrol";
    final static String S3_EXPIRATION_ATTR_KEY = "s3.expiration";
    final static String S3_STORAGECLASS_ATTR_KEY = "s3.storeClass";
    final static String S3_USERMETA_ATTR_KEY = "s3.usermetadata";
    final static String S3_API_METHOD_ATTR_KEY = "s3.apimethod";
    final static String S3_API_METHOD_PUTOBJECT = "putobject";
    final static String S3_API_METHOD_MULTIPARTUPLOAD = "multipartupload";
    final static String S3_SSE_ALGORITHM = "s3.sseAlgorithm";
    final static String S3_ENCRYPTION_STRATEGY = "s3.encryptionStrategy";


    final static String S3_PROCESS_UNSCHEDULED_MESSAGE = "Processor unscheduled, stopping upload";

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final AmazonS3Client s3;

        try {
           s3  = getS3Client(context, flowFile.getAttributes());
        } catch (Exception e) {
            getLogger().error("Failed to initialize S3 client", e);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final long startNanos = System.nanoTime();

        final String bucket = context.getProperty(BUCKET_WITH_DEFAULT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        final String key = context.getProperty(KEY).evaluateAttributeExpressions(flowFile).getValue();
        final String uploadId = context.getProperty(UPLOAD_ID).evaluateAttributeExpressions(flowFile).getValue();
        final int part = context.getProperty(PART_NUMBER).evaluateAttributeExpressions(flowFile).asInteger();
        final String md5Digest = context.getProperty(MD5_DIGEST).evaluateAttributeExpressions(flowFile).getValue();

        final FlowFile ff = flowFile;
        final Map<String, String> attributes = new HashMap<>();
        final String ffFilename = ff.getAttributes().get(CoreAttributes.FILENAME.key());
        final ResourceTransferSource resourceTransferSource = context.getProperty(RESOURCE_TRANSFER_SOURCE).asAllowableValue(ResourceTransferSource.class);

        attributes.put(S3_BUCKET_KEY, bucket);
        attributes.put(S3_OBJECT_KEY, key);

        /*
         * If necessary, run age off for existing uploads in AWS S3 and local state
         */
        // ageoffS3Uploads(context, s3, now, bucket);

        /*
         * Then
         */
        try {
            final FlowFile flowFileCopy = flowFile;
            Optional<FileResource> optFileResource = getFileResource(resourceTransferSource, context, flowFile.getAttributes());
            try (InputStream in = optFileResource
                    .map(FileResource::getInputStream)
                    .orElseGet(() -> session.read(flowFileCopy))) {
                final ObjectMetadata objectMetadata = new ObjectMetadata();
                objectMetadata.setContentLength(optFileResource.map(FileResource::getSize).orElseGet(ff::getSize));

                final String contentType = context.getProperty(CONTENT_TYPE)
                        .evaluateAttributeExpressions(ff).getValue();
                if (contentType != null) {
                    objectMetadata.setContentType(contentType);
                    attributes.put(S3_CONTENT_TYPE, contentType);
                }

                final String cacheControl = context.getProperty(CACHE_CONTROL)
                        .evaluateAttributeExpressions(ff).getValue();
                if (cacheControl != null) {
                    objectMetadata.setCacheControl(cacheControl);
                    attributes.put(S3_CACHE_CONTROL, cacheControl);
                }

                final String contentDisposition = context.getProperty(CONTENT_DISPOSITION).getValue();
                String fileName = URLEncoder.encode(ff.getAttribute(CoreAttributes.FILENAME.key()), StandardCharsets.UTF_8);
                if (contentDisposition != null && contentDisposition.equals(CONTENT_DISPOSITION_INLINE)) {
                    objectMetadata.setContentDisposition(CONTENT_DISPOSITION_INLINE);
                    attributes.put(S3_CONTENT_DISPOSITION, CONTENT_DISPOSITION_INLINE);
                } else if (contentDisposition != null && contentDisposition.equals(CONTENT_DISPOSITION_ATTACHMENT)) {
                    String contentDispositionValue = CONTENT_DISPOSITION_ATTACHMENT + "; filename=\"" + fileName + "\"";
                    objectMetadata.setContentDisposition(contentDispositionValue);
                    attributes.put(S3_CONTENT_DISPOSITION, contentDispositionValue);
                } else {
                    objectMetadata.setContentDisposition(fileName);
                }

                final String expirationRule = context.getProperty(EXPIRATION_RULE_ID)
                        .evaluateAttributeExpressions(ff).getValue();
                if (expirationRule != null) {
                    objectMetadata.setExpirationTimeRuleId(expirationRule);
                }

                final Map<String, String> userMetadata = new HashMap<>();
                for (final Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
                    if (entry.getKey().isDynamic()) {
                        final String value = context.getProperty(
                                entry.getKey()).evaluateAttributeExpressions(ff).getValue();
                        userMetadata.put(entry.getKey().getName(), value);
                    }
                }

                final String serverSideEncryption = context.getProperty(SERVER_SIDE_ENCRYPTION).getValue();
                AmazonS3EncryptionService encryptionService = null;

                if (!serverSideEncryption.equals(NO_SERVER_SIDE_ENCRYPTION)) {
                    objectMetadata.setSSEAlgorithm(serverSideEncryption);
                    attributes.put(S3_SSE_ALGORITHM, serverSideEncryption);
                } else {
                    encryptionService = context.getProperty(ENCRYPTION_SERVICE).asControllerService(AmazonS3EncryptionService.class);
                }

                if (!userMetadata.isEmpty()) {
                    objectMetadata.setUserMetadata(userMetadata);
                }

                //----------------------------------------
                // multipart upload
                //----------------------------------------

                // upload parts
                //------------------------------------------------------------
                long thisPartSize;
                // boolean isLastPart;
                // for (int part = currentState.getPartETags().size() + 1;
                //         currentState.getFilePosition() < currentState.getContentLength(); part++) {
                if (!UploadS3Part.this.isScheduled()) {
                    throw new IOException(S3_PROCESS_UNSCHEDULED_MESSAGE + " flowfile=" + ffFilename +
                            " part=" + part + " uploadId=" + uploadId);
                }
                thisPartSize = objectMetadata.getContentLength();
                // isLastPart = currentState.getContentLength() == currentState.getFilePosition() + thisPartSize;
                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(bucket)
                        .withKey(key)
                        .withUploadId(uploadId)
                        .withInputStream(in)
                        .withPartNumber(part)
                        .withPartSize(thisPartSize);

                if (md5Digest != null) {
                    // convert hex -> bytes
                    byte[] md5Bytes = DatatypeConverter.parseHexBinary(flowFile.getAttribute("content_MD5"));
                    // encode bytes to base64
                    String md5Base64 = Base64.getEncoder().encodeToString(md5Bytes);
                    uploadRequest.withMD5Digest(md5Base64);
                }

                if (encryptionService != null) {
                    encryptionService.configureUploadPartRequest(uploadRequest, objectMetadata);
                }
                try {
                    UploadPartResult uploadPartResult = s3.uploadPart(uploadRequest);

                    int available = 0;
                    try {
                        available = in.available();
                    } catch (IOException ignored) {
                        // in case of the last part, the stream is already closed
                    }
                    getLogger().info("Success uploading part flowfile={} part={} available={} etag={} uploadId={}",
                            ffFilename, part, available, uploadPartResult.getETag(), uploadId);
                    if (uploadPartResult.getETag() != null) {
                        attributes.put(S3_ETAG_ATTR_KEY, uploadPartResult.getETag());
                        attributes.put(S3_PART_NUMBER_ATTR_KEY, String.valueOf(uploadPartResult.getPartNumber()));
                    }
                } catch (AmazonClientException e) {
                    getLogger().info("Failure uploading part flowfile={} part={} bucket={} key={}", ffFilename, part, bucket, key, e);
                    throw (e);
                }

                
            } catch (IOException e) {
                getLogger().error("Error during upload of flow files", e);
                throw e;
            }

            final String url = s3.getResourceUrl(bucket, key);
            attributes.put("s3.url", url);
            flowFile = session.putAllAttributes(flowFile, attributes);
            session.transfer(flowFile, REL_SUCCESS);

            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            session.getProvenanceReporter().send(flowFile, url, millis);

            getLogger().info("Successfully put {} to Amazon S3 in {} milliseconds", ff, millis);

        } catch (final IllegalArgumentException | ProcessException | AmazonClientException | IOException e) {
            extractExceptionDetails(e, session, flowFile);
            if (e.getMessage().contains(S3_PROCESS_UNSCHEDULED_MESSAGE)) {
                getLogger().info(e.getMessage());
                session.rollback();
            } else {
                getLogger().error("Failed to put {} to Amazon S3 due to {}", flowFile, e);
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

}
