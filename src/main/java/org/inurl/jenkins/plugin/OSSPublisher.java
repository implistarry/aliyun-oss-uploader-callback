package org.inurl.jenkins.plugin;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnull;

import com.aliyun.oss.OSSClient;
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

    private List<String> channelLabelList;

    private final String callbackUrl;

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
                        String channelLabel) {
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
        logger.println("回调地址 =>" + callbackUrl);
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
        callback(false, "上传失败", "", logger, key);
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
        String realKey = key;
        if (realKey.startsWith("/")) {
            realKey = realKey.substring(1);
        }

        InputStream inputStream = path.read();
        logger.println("uploading [" + path.getRemote() + "] to [" + realKey + "]");
        client.putObject(bucketName, realKey, inputStream);

        callback(true, "上传成功", realKey, logger, key);
    }

    private void callback(boolean success, String msg, String path, PrintStream logger, String key) {
        logger.println("gameId: " + gameId);
        logger.println("gameName: " + gameName);
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
