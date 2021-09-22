package org.inurl.jenkins.plugin;


import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.aliyun.oss.*;
import com.aliyun.oss.model.CompleteMultipartUploadRequest;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.InitiateMultipartUploadRequest;
import com.aliyun.oss.model.InitiateMultipartUploadResult;
import com.aliyun.oss.model.ListPartsRequest;
import com.aliyun.oss.model.PartETag;
import com.aliyun.oss.model.PartListing;
import com.aliyun.oss.model.PartSummary;
import com.aliyun.oss.model.UploadPartRequest;
import com.aliyun.oss.model.UploadPartResult;

/**
 * This sample demonstrates how to upload multiparts to Aliyun OSS
 * using the OSS SDK for Java.
 * 本地部署稳定,线上服务上传不稳定
 */
public class MultipartUpload {

    private static OSS client = null;

    private static ExecutorService executorService ;
    private static List<PartETag> partETags;
    private PrintStream logger;
    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private String bucketName;
    private String realKey;

    public interface MPCallback{
        void callback(boolean success, String msg, String path, PrintStream logger, String key, String localFilePath);
        void fileIsTooLarge() throws IOException, InterruptedException;
    }

    public MultipartUpload(String accessKeyId,
                           String accessKeySecret,
                           String endpoint,
                           String bucketName,PrintStream logger) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.endpoint = endpoint;
        this.bucketName = bucketName;
        this.logger = logger;

        ClientBuilderConfiguration conf = new ClientBuilderConfiguration();
        conf.setIdleConnectionTime(1000);
        client = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret, conf);
    }

    public void upload(String localFilePath,String key,MPCallback callback) throws IOException {
        executorService = Executors.newFixedThreadPool(5);
        if(partETags!=null){
            partETags.clear();
        }else{
            partETags = Collections.synchronizedList(new ArrayList<PartETag>());
        }
        try {
            realKey = key;
            if (realKey.startsWith("/")) {
                realKey = realKey.substring(1);
            }
            /*
             * Claim a upload id firstly
             */
            String uploadId = claimUploadId();
            logger.println("分片上传事件的唯一标识: " + uploadId + "\n");

            /*
             * Calculate how many parts to be divided
             */
            final long partSize = 1 * 1024 * 1024L;   // 1MB
            final File sampleFile = new File(localFilePath);
            long fileLength = sampleFile.length();
            int partCount = (int) (fileLength / partSize);
            if (fileLength % partSize != 0) {
                partCount++;
            }
            if (partCount > 10000) {
                logger.println("文件分片总数("+partCount+")超过最大可分片数10000,将采用单文件方式上传");
                callback.fileIsTooLarge();
//                throw new RuntimeException("分片文件总数不应超过 10000");
            } else {
                logger.println("文件分片总数 " + partCount + "\n");
            }

            /*
             * Upload multiparts to your bucket
             */
            logger.println("开始上传文件分片到OSS\n");
            for (int i = 0; i < partCount; i++) {
                long startPos = i * partSize;
                long curPartSize = (i + 1 == partCount) ? (fileLength - startPos) : partSize;
                executorService.execute(new PartUploader(sampleFile,
                        startPos,
                        curPartSize,
                        i + 1,
                        uploadId,bucketName,realKey,logger));
            }

            /*
             * Waiting for all parts finished
             */
            executorService.shutdown();
            while (!executorService.isTerminated()) {
                try {
                    executorService.awaitTermination(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    logger.println("ERROR to exec executorService.awaitTermination");
                    logger.println(e.toString());
                }
            }

            /*
             * Verify whether all parts are finished
             */
            if (partETags.size() != partCount) {
                logger.println("partETags.size():"+partETags.size()+"  partCount:"+partCount);
                throw new IllegalStateException("Upload multiparts fail due to some parts are not finished yet");
            } else {
                logger.println("Succeed to complete multiparts into an object named " + realKey + "\n");
            }

            /*
             * View all parts uploaded recently
             */
            listAllParts(uploadId);

            /*
             * Complete to upload multiparts
             */
            completeMultipartUpload(uploadId);

            /*
             * Fetch the object that newly created at the step below.
             */
//            logger.println("Fetching an object");
//            client.getObject(new GetObjectRequest(bucketName, realKey), new File(localFilePath));
            callback.callback(true,"上传成功", realKey, logger, key, localFilePath);
        } catch (OSSException oe) {
            logger.println("Caught an OSSException, which means your request made it to OSS, "
                    + "but was rejected with an error response for some reason.");
            logger.println("Error Message: " + oe.getErrorMessage());
            logger.println("Error Code:       " + oe.getErrorCode());
            logger.println("Request ID:      " + oe.getRequestId());
            logger.println("Host ID:           " + oe.getHostId());
        } catch (ClientException ce) {
            logger.println("Caught an ClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with OSS, "
                    + "such as not being able to access the network.");
            logger.println("Error Message: " + ce.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            /*
             * Do not forget to shut down the client finally to release all allocated resources.
             */
//            if (client != null) {
//                client.shutdown();
//            }
        }
    }

    public void finishUpload(){
        if (client != null) {
            client.shutdown();
        }
    }

    private static class PartUploader implements Runnable {

        private File localFile;
        private long startPos;

        private long partSize;
        private int partNumber;
        private String uploadId;
        private String bucketName;
        private String key;
        private PrintStream logger;

        public PartUploader(File localFile,
                            long startPos,
                            long partSize,
                            int partNumber,
                            String uploadId,
                            String bucketName,
                            String key, PrintStream logger) {
            this.localFile = localFile;
            this.startPos = startPos;
            this.partSize = partSize;
            this.partNumber = partNumber;
            this.uploadId = uploadId;
            this.bucketName = bucketName;
            this.key = key;
            this.logger = logger;
        }

        @Override
        public void run() {
            InputStream instream = null;
            try {
                instream = new FileInputStream(this.localFile);
                instream.skip(this.startPos);

                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setBucketName(bucketName);
                uploadPartRequest.setKey(key);
                uploadPartRequest.setUploadId(this.uploadId);
                uploadPartRequest.setInputStream(instream);
                uploadPartRequest.setPartSize(this.partSize);
                uploadPartRequest.setPartNumber(this.partNumber);

                UploadPartResult uploadPartResult = client.uploadPart(uploadPartRequest);
                logger.print(" --> Part#" + this.partNumber + " done\n");
                synchronized (partETags) {
                    partETags.add(uploadPartResult.getPartETag());
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (instream != null) {
                    try {
                        instream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private String claimUploadId() {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, realKey);
        InitiateMultipartUploadResult result = client.initiateMultipartUpload(request);
        return result.getUploadId();
    }

    private void completeMultipartUpload(String uploadId) {
        // Make part numbers in ascending order
        Collections.sort(partETags, new Comparator<PartETag>() {

            @Override
            public int compare(PartETag p1, PartETag p2) {
                return p1.getPartNumber() - p2.getPartNumber();
            }
        });

        logger.println("Completing to upload multiparts\n");
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, realKey, uploadId, partETags);
        client.completeMultipartUpload(completeMultipartUploadRequest);
    }

    private void listAllParts(String uploadId) {
        logger.println("Listing all parts......");
        ListPartsRequest listPartsRequest = new ListPartsRequest(bucketName, realKey, uploadId);
        PartListing partListing = client.listParts(listPartsRequest);

        int partCount = partListing.getParts().size();
        for (int i = 0; i < partCount; i++) {
            PartSummary partSummary = partListing.getParts().get(i);
            logger.println("\tPart#" + partSummary.getPartNumber() + ", ETag=" + partSummary.getETag());
        }
        logger.println();
    }
}