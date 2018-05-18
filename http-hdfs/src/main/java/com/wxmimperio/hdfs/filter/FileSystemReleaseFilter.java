package com.wxmimperio.hdfs.filter;


import com.wxmimperio.hdfs.service.FileSystemAccess;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import java.io.IOException;

@Component
public abstract class FileSystemReleaseFilter implements Filter {

    private static final ThreadLocal<FileSystem> FILE_SYSTEM_TL = new ThreadLocal<>();

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        try {
            filterChain.doFilter(servletRequest, servletResponse);
        } finally {
            FileSystem fs = FILE_SYSTEM_TL.get();
            System.out.println("fs.........");
            if (fs != null) {
                System.out.println("fs......!= null...");
                FILE_SYSTEM_TL.remove();
                getFileSystemAccess().releaseFileSystem(fs);
            }
        }
    }

    @Override
    public void destroy() {

    }

    /**
     * Static method that sets the <code>FileSystem</code> to release back to
     * the {@link FileSystemAccess} service on servlet request completion.
     *
     * @param fs fileystem instance.
     */
    public static void setFileSystem(FileSystem fs) {
        System.out.println("set fs");
        FILE_SYSTEM_TL.set(fs);
    }

    /**
     * Abstract method to be implemetned by concrete implementations of the
     * filter that return the {@link FileSystemAccess} service to which the filesystem
     * will be returned to.
     *
     * @return the FileSystemAccess service.
     */
    protected abstract FileSystemAccess getFileSystemAccess();
}
