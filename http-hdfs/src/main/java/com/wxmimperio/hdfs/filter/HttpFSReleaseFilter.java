package com.wxmimperio.hdfs.filter;

import com.wxmimperio.hdfs.HttpHdfsApplication;
import com.wxmimperio.hdfs.service.FileSystemAccess;
import org.springframework.stereotype.Component;

import javax.servlet.annotation.WebFilter;

@Component
@WebFilter(filterName = "httpFSReleaseFilter", urlPatterns = "/*")
public class HttpFSReleaseFilter extends FileSystemReleaseFilter {

    @Override
    protected FileSystemAccess getFileSystemAccess() {
        return HttpHdfsApplication.context.getBean(FileSystemAccess.class);
    }
}
