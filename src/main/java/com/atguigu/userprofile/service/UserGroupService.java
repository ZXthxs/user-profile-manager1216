package com.atguigu.userprofile.service;

import com.atguigu.userprofile.bean.UserGroup;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.IService;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

public interface UserGroupService  extends IService<UserGroup> {

    void saveToMySql(UserGroup userGroup);

    void saveToCk(UserGroup userGroup);

    UserGroup saveToRedis(UserGroup userGroup);

    Long priSumNum(UserGroup userGroup);
    UserGroup updateCkUserGroup(String id,String busiDate);
}
