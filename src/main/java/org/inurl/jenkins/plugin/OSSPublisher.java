package org.inurl.jenkins.plugin;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.event.ProgressEvent;
import com.aliyun.oss.event.ProgressEventType;
import com.aliyun.oss.event.ProgressListener;
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

    @Override
    public void perform(@Nonnull Run<?, ?> run, @Nonnull FilePath workspace, @Nonnull Launcher launcher,
                        @Nonnull TaskListener listener) throws InterruptedException, IOException {
        PrintStream logger = listener.getLogger();
        envVars = run.getEnvironment(listener);
        OSSClient client = new OSSClient(endpoint, accessKeyId, accessKeySecret.getPlainText());

        String channelLabelEx = envVars.expand(channelLabel);
        channelLabelList = channelLabelEx != null ? new ArrayList<String>(Arrays.asList(channelLabelEx.split(","))) : new ArrayList<>();

        String local = localPath.substring(1);
        logger.println("回调地址 :" + callbackUrl + " ==> " + envVars.expand(callbackUrl));
        String[] remotes = remotePath.split(",");
        for (String remote : remotes) {
            remote = remote.substring(1);
            String expandLocal = envVars.expand(local);
            String expandRemote = envVars.expand(remote);
            logger.println("expandLocalPath =>" + expandLocal);
            logger.println("expandRemotePath =>" + expandRemote);
            FilePath p = new FilePath(workspace, expandLocal);
            getFilesNum(p.toString());
            logger.println("待上传文件数量: " + fileNum);
            if (p.isDirectory()) {
                logger.println("upload dir => " + p);
                upload(client, logger, expandRemote, p, true);
                logger.println("upload dir success");
            } else {
                logger.println("upload file => " + p);
                uploadFile(client, logger, expandRemote, p);
                logger.println("upload file success");
            }
        }
    }

    private int fileNum;

    public void getFilesNum(String path) {
        System.out.println("传入的路径: " + path);
        File file = new File(path);
        System.out.println("生成文件的路径: " + file.getAbsolutePath());
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
                logger.println("upload retrying (" + retries + "/" + maxRetries + ")");
            }
            try {
                uploadFile0(client, logger, key, path);
                return;
            } catch (Exception e) {
                e.printStackTrace(logger);
            }
        } while ((++retries) <= maxRetries);
        callback(false, "上传失败", "", logger, key, path.getRemote());
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
        logger.println("上传方式:" + (multipartUpload ? "分片上传" : "简单上传"));
        if (multipartUpload) {
            //分片上传
            multipartUpload(client, logger, key, path);
            return;
        }
        String realKey = key;
        if (realKey.startsWith("/")) {
            realKey = realKey.substring(1);
        }

        InputStream inputStream = path.read();
        logger.println("uploading [" + path.getRemote() + "] to [" + realKey + "]");
        client.putObject(bucketName, realKey, inputStream);

        callback(true, "上传成功", realKey, logger, key, path.getRemote());
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
        logger.println("回调结果: " + response);

        if (isDelete) {
            File file = new File(localFilePath);
            if (file.exists()) {
                boolean isDe = file.delete();
                logger.println("删除" + (isDe ? "[成功] ==> " : "[失败] ==> ") + "[" + localFilePath + "]");
            }
        }
    }

    /**
     * 分片上传
     */
    private void multipartUpload(OSSClient ossClient, PrintStream logger, String key, FilePath path)
            throws IOException {
        String realKey = key;
        if (realKey.startsWith("/")) {
            realKey = realKey.substring(1);
        }
        logger.println("分片上传  uploading [" + path.getRemote() + "] to [" + realKey + "]");
        // 创建InitiateMultipartUploadRequest对象。
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, realKey);
        // 如果需要在初始化分片时设置文件存储类型，请参考以下示例代码。
        // ObjectMetadata metadata = new ObjectMetadata();
        // metadata.setHeader(OSSHeaders.OSS_STORAGE_CLASS, StorageClass.Standard.toString());
        // request.setObjectMetadata(metadata);

        // 初始化分片。
        InitiateMultipartUploadResult upresult
                = ossClient.initiateMultipartUpload(request.withProgressListener(new UploadProgressLisenter(logger)));
        // 返回uploadId，它是分片上传事件的唯一标识。您可以根据该uploadId发起相关的操作，例如取消分片上传、查询分片上传等。
        String uploadId = upresult.getUploadId();

        // partETags是PartETag的集合。PartETag由分片的ETag和分片号组成。
        List<PartETag> partETags = new ArrayList<PartETag>();
        // 每个分片的大小，用于计算文件有多少个分片。单位为字节。
        final long partSize = 10 * 1024 * 1024L;   //10MB。

        // 填写本地文件的完整路径。如果未指定本地路径，则默认从示例程序所属项目对应本地路径中上传文件。
        final File sampleFile = new File(path.getRemote());
        long fileLength = sampleFile.length();
        int partCount = (int) (fileLength / partSize);
        if (fileLength % partSize != 0) {
            partCount++;
        }
        // 遍历分片上传。
        for (int i = 0; i < partCount; i++) {
            long startPos = i * partSize;
            long curPartSize = (i + 1 == partCount) ? (fileLength - startPos) : partSize;
            InputStream instream = new FileInputStream(sampleFile);
            // 跳过已经上传的分片。
            instream.skip(startPos);
            UploadPartRequest uploadPartRequest = new UploadPartRequest();
            uploadPartRequest.setBucketName(bucketName);
            uploadPartRequest.setKey(realKey);
            uploadPartRequest.setUploadId(uploadId);
            uploadPartRequest.setInputStream(instream);
            // 设置分片大小。除了最后一个分片没有大小限制，其他的分片最小为100 KB。
            uploadPartRequest.setPartSize(curPartSize);
            // 设置分片号。每一个上传的分片都有一个分片号，取值范围是1~10000，如果超出此范围，OSS将返回InvalidArgument错误码。
            uploadPartRequest.setPartNumber(i + 1);
            // 每个分片不需要按顺序上传，甚至可以在不同客户端上传，OSS会按照分片号排序组成完整的文件。
            UploadPartResult uploadPartResult = ossClient.uploadPart(uploadPartRequest);
            // 每次上传分片之后，OSS的返回结果包含PartETag。PartETag将被保存在partETags中。
            partETags.add(uploadPartResult.getPartETag());
        }


        // 创建CompleteMultipartUploadRequest对象。
        // 在执行完成分片上传操作时，需要提供所有有效的partETags。OSS收到提交的partETags后，会逐一验证每个分片的有效性。当所有的数据分片验证通过后，OSS将把这些分片组合成一个完整的文件。
        CompleteMultipartUploadRequest completeMultipartUploadRequest =
                new CompleteMultipartUploadRequest(bucketName, realKey, uploadId, partETags);
        // 如果需要在完成文件上传的同时设置文件访问权限，请参考以下示例代码。
        // completeMultipartUploadRequest.setObjectACL(CannedAccessControlList.PublicRead);

        // 完成上传。
        CompleteMultipartUploadResult completeMultipartUploadResult = ossClient.completeMultipartUpload(completeMultipartUploadRequest);
        System.out.println(completeMultipartUploadResult.getETag());
        // 关闭OSSClient。
//        ossClient.shutdown();
        callback(true, "上传成功", realKey, logger, key, path.getRemote());
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
