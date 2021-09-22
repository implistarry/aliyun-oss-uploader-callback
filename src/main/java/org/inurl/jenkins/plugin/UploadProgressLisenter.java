package org.inurl.jenkins.plugin;

import com.aliyun.oss.event.ProgressEvent;
import com.aliyun.oss.event.ProgressEventType;
import com.aliyun.oss.event.ProgressListener;

import java.io.PrintStream;

public class UploadProgressLisenter implements ProgressListener {
    private PrintStream logger;

    public UploadProgressLisenter(PrintStream logger) {
        this.logger = logger;
    }

    private long bytesWritten = 0;
    private long totalBytes = -1;
    private boolean succeed = false;

    @Override
    public void progressChanged(ProgressEvent progressEvent) {
        long bytes = progressEvent.getBytes();
        ProgressEventType eventType = progressEvent.getEventType();
        switch (eventType) {
            case TRANSFER_STARTED_EVENT:
                logger.println("开始上传......");
                break;
            case REQUEST_CONTENT_LENGTH_EVENT:
                this.totalBytes = bytes;
                logger.println(this.totalBytes + " bytes in total will be uploaded to OSS");
                break;
            case REQUEST_BYTE_TRANSFER_EVENT:
                this.bytesWritten += bytes;
                if (this.totalBytes != -1) {
                    int percent = (int) (this.bytesWritten * 100.0 / this.totalBytes);
                    logger.println(bytes + " bytes have been written at this time, upload progress: " + percent + "%(" + this.bytesWritten + "/" + this.totalBytes + ")");
                } else {
                    logger.println(bytes + " bytes have been written at this time, upload ratio: unknown" + "(" + this.bytesWritten + "/...)");
                }
                break;
            case TRANSFER_COMPLETED_EVENT:
                this.succeed = true;
                logger.println("上传成功, " + this.bytesWritten + " bytes have been transferred in total");
                break;
            case TRANSFER_FAILED_EVENT:
                logger.println("上传失败, " + this.bytesWritten + " bytes have been transferred");
                break;
            default:
                break;
        }
    }
}
