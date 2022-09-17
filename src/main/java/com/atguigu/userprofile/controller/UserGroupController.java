package com.atguigu.userprofile.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.userprofile.bean.TagCondition;
import com.atguigu.userprofile.bean.TaskInfo;
import com.atguigu.userprofile.bean.UserGroup;
import com.atguigu.userprofile.service.UserGroupService;
import com.atguigu.userprofile.utils.RedisUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import io.swagger.annotations.ApiOperation;
import org.apache.catalina.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.List;

/**
 * <p>
 *  前端控制器
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */
@RestController
public class UserGroupController {

    @Autowired
    UserGroupService userGroupService;

    @RequestMapping("/user-group-list")
    @CrossOrigin
    public String  getUserGroupList(@RequestParam("pageNo")int pageNo , @RequestParam("pageSize") int pageSize){
        int startNo=(  pageNo-1)* pageSize;
        int endNo=startNo+pageSize;

        QueryWrapper<UserGroup> queryWrapper = new QueryWrapper<>();
        int count = userGroupService.count(queryWrapper);

        queryWrapper.orderByDesc("id").last(" limit " + startNo + "," + endNo);
        List<UserGroup> userGroupList = userGroupService.list(queryWrapper);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("detail",userGroupList);
        jsonObject.put("total",count);

        return  jsonObject.toJSONString();
    }

    @PostMapping("/user-group")
    public String CrowdSegmentation(@RequestBody UserGroup userGroup){

        // 1.把分群信息存入Mysql。 把条件信息，转为json，调用 UserGroupMapper 把 收到的 userGroup 存入Mysql
        userGroupService.saveToMySql(userGroup);

        userGroupService.saveToCk(userGroup);

        UserGroup userGroup1 = userGroupService.saveToRedis(userGroup);

        //手动模拟一个异常，让程序每次到这里就中止，不返回数据给前端

        userGroup1.setCreateTime(new Date());

        userGroupService.saveOrUpdate(userGroup1);

        return "success";

    }

    @PostMapping("/user-group-evaluate")
    public Long evaluateUserGroup(@RequestBody UserGroup userGroup){

        Long num = userGroupService.priSumNum(userGroup);

        return num;

    }

    @PostMapping("/user-group-refresh/{id}")
    public String updateUserGroup(@PathVariable String id,String busiDate){

        UserGroup userGroup = userGroupService.updateCkUserGroup(id, busiDate);

        UserGroup userGroup1 = userGroupService.saveToRedis(userGroup);

        userGroup1.setUpdateTime(new Date());

        userGroupService.saveOrUpdate(userGroup1);

        return "success";
    }


}

