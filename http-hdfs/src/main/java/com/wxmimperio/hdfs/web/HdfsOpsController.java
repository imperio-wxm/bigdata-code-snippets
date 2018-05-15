package com.wxmimperio.hdfs.web;

import com.google.common.collect.Lists;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("hdfs")
public class HdfsOpsController {

    @ApiOperation("获取用户信息")
    @GetMapping("list")
    public List<String> listFilePath(@RequestBody String path) {
        System.out.println(path);
        return Lists.newArrayList();
    }
}
