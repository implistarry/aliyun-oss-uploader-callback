package org.inurl.jenkins.plugin;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.*;
import hudson.Extension;
import hudson.FilePath;
import hudson.Launcher;
import hudson.EnvVars;
import hudson.model.AbstractProject;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.tasks.BuildStepDescriptor;
import hudson.tasks.BuildStepMonitor;
import hudson.tasks.Publisher;
import hudson.util.FormValidation;
import hudson.util.Secret;
import jenkins.tasks.SimpleBuildStep;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.jenkinsci.Symbol;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.QueryParameter;

public class OSSPublisher extends Publisher implements SimpleBuildStep {

    private final String endpoint;

    private final String accessKeyId;

    private final Secret accessKeySecret;

    private final String bucketName;

    private final String localPath;

    private final String remotePath;

    private final String maxRetries;

    private final String gameId;

    private final String gameName;

    private final String channelLabel;

    private final boolean isDelete;

    private final boolean multipartUpload;

    private List<String> channelLabelList;

    private final String callbackUrl;

    public boolean isDelete() {
        return isDelete;
    }

    public boolean getIsDelete() {
        return isDelete;
    }

    public boolean isMultipartUpload() {
        return multipartUpload;
    }

    public String getGameId() {
        return gameId;
    }

    public String getGameName() {
        return gameName;
    }

    public String getChannelLabel() {
        return channelLabel;
    }

    public List<String> getChannelLabelList() {
        return channelLabelList;
    }

    public String getCallbackUrl() {
        return callbackUrl;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret.getPlainText();
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getLocalPath() {
        return localPath;
    }

    public String getRemotePath() {
        return remotePath;
    }

    public int getMaxRetries() {
        return StringUtils.isEmpty(maxRetries) ? 3 : Integer.parseInt(maxRetries);
    }

    @DataBoundConstructor
    public OSSPublisher(String endpoint, String accessKeyId, String accessKeySecret, String bucketName,
                        String localPath, String remotePath,
                        String maxRetries, String callbackUrl,
                        String gameId, String gameName,
                        String channelLabel, boolean isDelete, boolean multipartUpload) {
        this.endpoint = endpoint;
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = Secret.fromString(accessKeySecret);
        this.bucketName = bucketName;
        this.localPath = localPath;
        this.remotePath = remotePath;
        this.maxRetries = maxRetries;
        this.callbackUrl = callbackUrl;
        this.gameId = gameId;
        this.gameName = gameName;
        this.channelLabel = channelLabel;
        this.isDelete = isDelete;
        this.multipartUpload = multipartUpload;
    }

    @Override
    public BuildStepMonitor getRequiredMonitorService() {
        return BuildStepMonitor.NONE;
    }

    EnvVars envVars;
//    MultipartUpload multipartUploader;
    OSSClient ossClient;
    private Map<String, MultipartUpload> uploadMap;

    private MultipartUpload getMultipartUploader(String key) {
        if (uploadMap.containsKey(key)) {
            return uploadMap.get(key);
        } else {
            MultipartUpload upload = new MultipartUpload(accessKeyId, accessKeySecret.getPlainText(), endpoint, bucketName, logger);
            uploadMap.put(key, upload);
            return upload;
        }
    }

    PrintStream logger;

    @Override
    public void perform(@Nonnull Run<?, ?> run, @Nonnull FilePath workspace, @Nonnull Launcher launcher,
                        @Nonnull TaskListener listener) throws InterruptedException, IOException {
        logger = listener.getLogger();
        envVars = run.getEnvironment(listener);
        if (!isMultipartUpload()) {
//            multipartUploader = new MultipartUpload(accessKeyId, accessKeySecret.getPlainText(), endpoint, bucketName, logger);
//        } else {
            ossClient = new OSSClient(endpoint, accessKeyId, accessKeySecret.getPlainText());
        }

        String channelLabelEx = envVars.expand(channelLabel);
        channelLabelList = channelLabelEx != null ? new ArrayList<String>(Arrays.asList(channelLabelEx.split(","))) : new ArrayList<>();

        String local = localPath.substring(1);
        logger.println("???????????? :" + callbackUrl + " ==> " + envVars.expand(callbackUrl));
        String[] remotes = remotePath.split(",");
        for (String remote : remotes) {
            remote = remote.substring(1);
            String expandLocal = envVars.expand(local);
            String expandRemote = envVars.expand(remote);
            logger.println("expandLocalPath =>" + expandLocal);
            logger.println("expandRemotePath =>" + expandRemote);
            FilePath p = new FilePath(workspace, expandLocal);
            getFilesNum(p.toString());
            logger.println("?????????????????????: " + fileNum);
            if (p.isDirectory()) {
                logger.println("upload dir => " + p);
                upload(ossClient, logger, expandRemote, p, true);
                logger.println("upload dir success");
            } else {
                logger.println("upload file => " + p);
                uploadFile(ossClient, logger, expandRemote, p);
                logger.println("upload file success");
            }
        }
    }

    private int fileNum;

    public void getFilesNum(String path) {
        System.out.println("???????????????: " + path);
        File file = new File(path);
        System.out.println("?????????????????????: " + file.getAbsolutePath());
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null != files) {
                for (File file2 : files) {
                    if (file2.isDirectory()) {
                        getFilesNum(file2.getAbsolutePath());
                    } else {
                        fileNum++;
                    }
                }
            }
        } else {
            fileNum = 0;
        }
    }

    private void upload(OSSClient client, PrintStream logger, String base, FilePath path, boolean root)
            throws InterruptedException, IOException {
        if (path.isDirectory()) {
            for (FilePath f : path.list()) {
                upload(client, logger, base + (root ? "" : ("/" + path.getName())), f, false);
            }
            return;
        }
        if(isMultipartUpload()){
            uploadMap = new HashedMap();
        }
        uploadFile(client, logger, base + "/" + path.getName(), path);
    }


    private void uploadFile(OSSClient client, PrintStream logger, String key, FilePath path)
            throws InterruptedException, IOException {
        if (!path.exists()) {
            logger.println("file [" + path.getRemote() + "] not exists, skipped");
            return;
        }
        int maxRetries = getMaxRetries();
        int retries = 0;
        do {
            if (retries > 0) {
                logger.println("[!!!]upload retrying (" + retries + "/" + maxRetries + ")");
            }
            try {
                if (isMultipartUpload()) {
                    getMultipartUploader(path.getRemote()).upload(path.getRemote(), key, new MultipartUpload.MPCallback() {
                        @Override
                        public void callback(boolean success, String msg, String path, PrintStream logger, String key, String localFilePath) {
                            OSSPublisher.this.callback(success, msg, path, logger, key, localFilePath);
                        }

                        @Override
                        public void fileIsTooLarge() throws IOException, InterruptedException {
                            uploadFile0(client, logger, key, path);
                        }
                    });
                } else {
                    uploadFile0(client, logger, key, path);
                }
                return;
            } catch (Exception e) {
                e.printStackTrace(logger);
            }
        } while ((++retries) <= maxRetries);
        callback(false, "????????????", "", logger, key, path.getRemote());
        throw new RuntimeException("upload fail, more than the max of retries");
    }

    private String getChannelFromName(String fileName) {
        if (fileName == null) return "";
        for (int i = 0; i < channelLabelList.size(); i++) {
            if (fileName.contains(channelLabelList.get(i))) {
                String label = channelLabelList.get(i);
                channelLabelList.remove(i);
                return label;
            }
        }
        return "";
    }

    private void uploadFile0(OSSClient client, PrintStream logger, String key, FilePath path)
            throws InterruptedException, IOException {
        logger.println("????????????:" + (multipartUpload ? "????????????" : "????????????"));
        if (multipartUpload) {
            //????????????
            multipartUpload(client, logger, key, path);
            return;
        }
        String realKey = key;
        if (realKey.startsWith("/")) {
            realKey = realKey.substring(1);
        }

        InputStream inputStream = path.read();
        logger.println("uploading [" + path.getRemote() + "] to [" + realKey + "]");
//        client.putObject(bucketName, realKey, inputStream);
        client.putObject(new PutObjectRequest(bucketName, realKey, inputStream)
                .withProgressListener(new UploadProgressLisenter(logger)));
        callback(true, "????????????", realKey, logger, key, path.getRemote());
    }

    private void callback(boolean success, String msg, String path, PrintStream logger, String key, String localFilePath) {
        logger.println("gameId: " + gameId + " = " + envVars.expand(gameId));
        logger.println("gameName: " + gameName + " = " + envVars.expand(gameName));
        logger.println("path: " + path);
        logger.println("key: " + key);
        CallbackResult result = new CallbackResult();
        result.success = success;
        result.msg = msg;
        result.path = envVars.expand(path);
        result.gameId = envVars.expand(gameId);
        result.gameName = envVars.expand(gameName);
        result.channelLabel = getChannelFromName(key);
        result.remainingTaskNum = channelLabelList.size();
        String response = HttpURLConnectionUtil.post(envVars.expand(callbackUrl), result.getJson());
        logger.println("????????????: " + response);
        if (isMultipartUpload() && getMultipartUploader(path) != null && channelLabelList.size() == 0) {
            getMultipartUploader(path).finishUpload();
        }
        if (isDelete) {
            File file = new File(localFilePath);
            if (file.exists()) {
                boolean isDe = file.delete();
                logger.println("??????" + (isDe ? "[??????] ==> " : "[??????] ==> ") + "[" + localFilePath + "]");
            }
        }
    }

    /**
     * ????????????
     */
    private void multipartUpload(OSSClient ossClient, PrintStream logger, String key, FilePath path)
            throws IOException {
        String realKey = key;
        if (realKey.startsWith("/")) {
            realKey = realKey.substring(1);
        }
        logger.println("????????????  uploading [" + path.getRemote() + "] to [" + realKey + "]");
        // ??????InitiateMultipartUploadRequest?????????
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, realKey);
        // ??????????????????????????????????????????????????????????????????????????????????????????
        // ObjectMetadata metadata = new ObjectMetadata();
        // metadata.setHeader(OSSHeaders.OSS_STORAGE_CLASS, StorageClass.Standard.toString());
        // request.setObjectMetadata(metadata);

        // ??????????????????
        InitiateMultipartUploadResult upresult
                = ossClient.initiateMultipartUpload(request.withProgressListener(new UploadProgressLisenter(logger)));
        // ??????uploadId???????????????????????????????????????????????????????????????uploadId???????????????????????????????????????????????????????????????????????????
        String uploadId = upresult.getUploadId();

        // partETags???PartETag????????????PartETag????????????ETag?????????????????????
        List<PartETag> partETags = new ArrayList<PartETag>();
        // ?????????????????????????????????????????????????????????????????????????????????
        final long partSize = 10 * 1024 * 1024L;   //10MB???

        // ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        final File sampleFile = new File(path.getRemote());
        long fileLength = sampleFile.length();
        int partCount = (int) (fileLength / partSize);
        if (fileLength % partSize != 0) {
            partCount++;
        }
        // ?????????????????????
        for (int i = 0; i < partCount; i++) {
            long startPos = i * partSize;
            long curPartSize = (i + 1 == partCount) ? (fileLength - startPos) : partSize;
            InputStream instream = new FileInputStream(sampleFile);
            // ??????????????????????????????
            instream.skip(startPos);
            UploadPartRequest uploadPartRequest = new UploadPartRequest();
            uploadPartRequest.setBucketName(bucketName);
            uploadPartRequest.setKey(realKey);
            uploadPartRequest.setUploadId(uploadId);
            uploadPartRequest.setInputStream(instream);
            // ??????????????????????????????????????????????????????????????????????????????????????????100 KB???
            uploadPartRequest.setPartSize(curPartSize);
            // ?????????????????????????????????????????????????????????????????????????????????1~10000???????????????????????????OSS?????????InvalidArgument????????????
            uploadPartRequest.setPartNumber(i + 1);
            // ??????????????????????????????????????????????????????????????????????????????OSS????????????????????????????????????????????????
            UploadPartResult uploadPartResult = ossClient.uploadPart(uploadPartRequest);
            // ???????????????????????????OSS?????????????????????PartETag???PartETag???????????????partETags??????
            partETags.add(uploadPartResult.getPartETag());
        }


        // ??????CompleteMultipartUploadRequest?????????
        // ??????????????????????????????????????????????????????????????????partETags???OSS???????????????partETags??????????????????????????????????????????????????????????????????????????????????????????OSS???????????????????????????????????????????????????
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, realKey, uploadId, partETags);
        // ???????????????????????????????????????????????????????????????????????????????????????????????????
        // completeMultipartUploadRequest.setObjectACL(CannedAccessControlList.PublicRead);

        // ???????????????
        CompleteMultipartUploadResult completeMultipartUploadResult = ossClient.completeMultipartUpload(completeMultipartUploadRequest);
        System.out.println(completeMultipartUploadResult.getETag());
        // ??????OSSClient???
//        ossClient.shutdown();
        callback(true, "????????????", realKey, logger, key, path.getRemote());
    }


    @Symbol("aliyunOSSUpload")
    @Extension
    public static final class DescriptorImpl extends BuildStepDescriptor<Publisher> {

        public FormValidation doCheckMaxRetries(@QueryParameter String value) {
            try {
                Integer.parseInt(value);
            } catch (Exception e) {
                return FormValidation.error(Messages.OSSPublish_MaxRetiesMustBeNumbers());
            }
            return FormValidation.ok();
        }

        public FormValidation doCheckEndpoint(@QueryParameter(required = true) String value) {
            return checkValue(value, Messages.OSSPublish_MissingEndpoint());
        }

        public FormValidation doCheckAccessKeyId(@QueryParameter(required = true) String value) {
            return checkValue(value, Messages.OSSPublish_MissingAccessKeyId());
        }

        public FormValidation doCheckAccessKeySecret(@QueryParameter(required = true) String value) {
            return checkValue(value, Messages.OSSPublish_MissingAccessKeySecret());
        }

        public FormValidation doCheckBucketName(@QueryParameter(required = true) String value) {
            return checkValue(value, Messages.OSSPublish_MissingBucketName());
        }

        public FormValidation doCheckLocalPath(@QueryParameter(required = true) String value) {
            return checkBeginWithSlash(value);
        }

        public FormValidation doCheckRemotePath(@QueryParameter(required = true) String value) {
            return checkBeginWithSlash(value);
        }

        private FormValidation checkBeginWithSlash(String value) {
            if (!value.startsWith("/")) {
                return FormValidation.error(Messages.OSSPublish_MustBeginWithSlash());
            }
            return FormValidation.ok();
        }

        private FormValidation checkValue(String value, String message) {
            if (StringUtils.isEmpty(value)) {
                return FormValidation.error(message);
            }
            return FormValidation.ok();
        }

        @Override
        public boolean isApplicable(Class<? extends AbstractProject> jobType) {
            return true;
        }

        @Nonnull
        @Override
        public String getDisplayName() {
            return Messages.OSSPublish_DisplayName();
        }
    }

}
