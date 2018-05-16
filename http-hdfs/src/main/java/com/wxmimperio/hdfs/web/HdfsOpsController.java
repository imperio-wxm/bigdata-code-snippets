package com.wxmimperio.hdfs.web;

import com.google.common.collect.Lists;
import com.wxmimperio.hdfs.config.HdfsConfig;
import com.wxmimperio.hdfs.service.FileSystemAccessService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiOperation;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.web.HttpUserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

@Api("HDFS操作")
@RestController
@RequestMapping("hdfs")
public class HdfsOpsController {

    private final HdfsConfig hdfsConfig;
    private final FileSystemAccessService fileSystemAccessService;
    private FileSystem fs;

    @Autowired
    public HdfsOpsController(HdfsConfig hdfsConfig, FileSystemAccessService fileSystemAccessService) {
        this.hdfsConfig = hdfsConfig;
        this.fileSystemAccessService = fileSystemAccessService;
    }

    @ApiOperation("获取文件列表")
    @ApiImplicitParam(name = "path", value = "hdfs路径")
    @GetMapping("list/{path}")
    public List<String> listFilePath(@PathVariable("path") String path, HttpServletResponse response) {
        path = path.replaceAll("\\|", "/");
        final Path inPath = new Path(path);

        System.out.println(path);

        try {
            UserGroupInformation user = UserGroupInformation.createRemoteUser("hadoop");
            //the real use is the one that has the Kerberos credentials needed for
            //SPNEGO to work
            user = user.getRealUser();
            if (user == null) {
                user = UserGroupInformation.getLoginUser();
            }
            fs = fileSystemAccessService.createFileSystem("hadoop", null);
            OutputStream out = null;
            out = user.doAs((PrivilegedExceptionAction<OutputStream>) () -> {
                SequenceFile.Reader reader = null;
                OutputStream outputStream = response.getOutputStream();
                if (fs.exists(inPath)) {
                    reader = new SequenceFile.Reader(fileSystemAccessService.getFileSystemConfiguration(), SequenceFile.Reader.file(inPath));
                    Writable inKey = (Writable) ReflectionUtils.newInstance(
                            reader.getKeyClass(), fileSystemAccessService.getFileSystemConfiguration());
                    Writable inValue = (Writable) ReflectionUtils.newInstance(
                            reader.getValueClass(), fileSystemAccessService.getFileSystemConfiguration());
                    long index = 0L;
                    while (reader.next(inKey, inValue)) {
                        if (index++ > 10) {
                            break;
                        }
                        System.out.println(inValue.toString());
                        outputStream.write(inValue.toString().getBytes());
                    }
                }
                return outputStream;
            });
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Lists.newArrayList();
    }

    public static List<String> getFileList(String dataPath, FileSystem fs) throws IOException {
        List<String> fileList = new ArrayList<>();
        try {
            Path path = new Path(dataPath);
            FileStatus[] fileStatusArray = fs.globStatus(path);
            if (fileStatusArray != null) {
                for (FileStatus fileStatus : fileStatusArray) {
                    if (fs.isFile(fileStatus.getPath())) {
                        String fullPath = fileStatus.getPath().toString();
                        fileList.add(fullPath);
                    } else if (fs.isDirectory(fileStatus.getPath())) {
                        for (FileStatus fileStatus2 : fs.listStatus(fileStatus.getPath())) {
                            if (fs.isFile(fileStatus2.getPath())) {
                                String fullPath = fileStatus2.getPath().toString();
                                fileList.add(fullPath);
                            } else {
                                throw new Exception("file path error: " + fileStatus2.getPath().toString());
                            }
                        }
                    }
                }
            }
            return fileList;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


    @ApiOperation("获取文件")
    @ApiImplicitParam(name = "name", value = "hdfs名称")
    @PostMapping("list/{name}")
    public void listFile(@PathVariable("name") String name) {
        try {
            fileSystemAccessService.releaseFileSystem(fs);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(name);
    }

    @ApiOperation("上传文件")
    @ApiImplicitParam(name = "name", value = "hdfs名称")
    @PutMapping("put/{name}")
    public void uploadFile(@PathVariable("name") String name) {
        System.out.println(name);
    }

    @ApiOperation("删除文件")
    @ApiImplicitParam(name = "name", value = "hdfs名称")
    @DeleteMapping("delete/{name}")
    public void deleteFile(@PathVariable("name") String name) {
        System.out.println(name);
    }
}
