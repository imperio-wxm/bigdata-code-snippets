package com.wxmimperio.hdfs.web;

import com.google.common.collect.Lists;
import com.wxmimperio.hdfs.config.HdfsConfig;
import com.wxmimperio.hdfs.service.FileSystemAccessService;
import io.swagger.annotations.*;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.*;


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

    private static String getFileName(String path, String returnFileType) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        returnFileType = !returnFileType.equalsIgnoreCase(".txt") ? "_" + returnFileType : returnFileType;
        return path.substring(
                path.lastIndexOf("/") + 1,
                path.lastIndexOf(".") < 0 ? path.length() : path.lastIndexOf(".")
        ) + "_" + sdf.format(new Date()) + returnFileType;
    }

    @ApiOperation("获取文件")
    @GetMapping("getFile/limit")
    public List<String> listFilePath(@RequestParam String path,
                                     @RequestParam(defaultValue = "10", required = false) Integer lineNum,
                                     @RequestParam(defaultValue = ".txt", required = false) String returnFileType,
                                     HttpServletResponse response) {
        final Path inPath = new Path(path);
        String seqPath = path + "_limit_" + UUID.randomUUID().toString().replaceAll("-", "");
        final Path sequenceTempPath = new Path(seqPath);
        String newLine = System.getProperty("line.separator");

        String fileName = getFileName(path, returnFileType);
        response.setHeader("content-type", "application/octet-stream");
        response.setContentType("application/octet-stream");
        response.setHeader("Content-Disposition", "attachment;filename=" + fileName);

        long startTime = System.currentTimeMillis();

        OutputStream out = null;
        try {
            UserGroupInformation user = UserGroupInformation.createRemoteUser("wxm");
            //the real use is the one that has the Kerberos credentials needed for
            //SPNEGO to work
            user = user.getRealUser();
            if (user == null) {
                user = UserGroupInformation.getLoginUser();
            }
            fs = fileSystemAccessService.createFileSystem("hadoop", null);
            out = user.doAs((PrivilegedExceptionAction<OutputStream>) () -> {
                SequenceFile.Reader reader = null;
                SequenceFile.Writer writer = null;

                if (returnFileType.equalsIgnoreCase("orc")) {
                    Text value = new Text();
                    Text key = new Text();
                    writer = SequenceFile.createWriter(
                            fileSystemAccessService.getFileSystemConfiguration(),
                            SequenceFile.Writer.file(sequenceTempPath),
                            SequenceFile.Writer.keyClass(key.getClass()),
                            SequenceFile.Writer.valueClass(value.getClass()),
                            SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
                    );
                }

                OutputStream outputStream = response.getOutputStream();
                if (fs.exists(inPath)) {
                    reader = new SequenceFile.Reader(fileSystemAccessService.getFileSystemConfiguration(), SequenceFile.Reader.file(inPath));
                    Writable inKey = (Writable) ReflectionUtils.newInstance(
                            reader.getKeyClass(), fileSystemAccessService.getFileSystemConfiguration());
                    Writable inValue = (Writable) ReflectionUtils.newInstance(
                            reader.getValueClass(), fileSystemAccessService.getFileSystemConfiguration());
                    long index = 0L;
                    while (reader.next(inKey, inValue)) {
                        if (lineNum != 0 && index++ > lineNum) {
                            break;
                        }
                        switch (returnFileType) {
                            case ".txt":
                                outputStream.write(inValue.toString().getBytes());
                                outputStream.write(newLine.getBytes());
                                break;
                            case "orc":
                                Objects.requireNonNull(writer).append(inKey, inValue);
                                break;
                            default:
                                break;
                        }
                    }
                    org.apache.hadoop.io.IOUtils.closeStream(writer);
                    if (returnFileType.equalsIgnoreCase("orc")) {
                        FSDataInputStream fsDataInputStream = fs.open(sequenceTempPath);
                        org.apache.hadoop.io.IOUtils.copyBytes(fsDataInputStream, outputStream, 4096, false);
                    }
                }
                return outputStream;
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (out != null) {
                try {
                    fileSystemAccessService.releaseFileSystem(fs);
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("cost = " + (System.currentTimeMillis() - startTime) + " ms");
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
