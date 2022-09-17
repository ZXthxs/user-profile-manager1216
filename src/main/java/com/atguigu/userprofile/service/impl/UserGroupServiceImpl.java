package com.atguigu.userprofile.service.impl;

import com.alibaba.fastjson.JSON;
import com.atguigu.userprofile.bean.TagCondition;
import com.atguigu.userprofile.bean.TagInfo;
import com.atguigu.userprofile.bean.UserGroup;
import com.atguigu.userprofile.constants.ConstCodes;
import com.atguigu.userprofile.mapper.UserGroupMapper;
import com.atguigu.userprofile.service.TagInfoService;
import com.atguigu.userprofile.service.UserGroupService;
import com.atguigu.userprofile.utils.RedisUtil;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import lombok.extern.slf4j.Slf4j;

import org.apache.catalina.User;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */
@Service
@Slf4j
@DS("mysql")
public class UserGroupServiceImpl extends ServiceImpl<UserGroupMapper, UserGroup> implements UserGroupService {
    @Autowired
    private  TagInfoService tagInfoService;

    @Override
    public void saveToMySql(UserGroup userGroup) {
        List<TagCondition> tagConditions = userGroup.getTagConditions();
        String string = JSON.toJSONString(tagConditions);
        userGroup.setConditionJsonStr(string);

        String s = userGroup.conditionJsonToComment();
        userGroup.setConditionComment(s);
        //保存进Mysql
        saveOrUpdate(userGroup);
    }

    @Override
    public void saveToCk(UserGroup userGroup) {
        String saveSql = getSaveSql(userGroup);
        super.baseMapper.saveToCk(userGroup.getId().toString(),saveSql);
    }
    @Override
    public UserGroup saveToRedis(UserGroup userGroup) {
        String saveSql = getSaveSql(userGroup);
        String groupId = userGroup.getId().toString();

        List<String> userGroupList = super.baseMapper.getUserGroupFromCk(userGroup, saveSql);

        Jedis jedis = RedisUtil.getJedis();


        String key = "user_group" + groupId;
        jedis.del(key);
        for (String s : userGroupList) {
            jedis.sadd(key, s);
        }

        jedis.close();
        userGroup.setUserGroupNum(userGroupList.size()+0L);
        return userGroup;
    }
    //预估分群人数

    @Override
    public Long priSumNum(UserGroup userGroup) {
        String saveSql = getSaveSql(userGroup);
        Long userGroupNum = super.baseMapper.getUserGroupNum(userGroup, saveSql);
        return userGroupNum;
    }

    //更新ck分群
    @Override
    public UserGroup updateCkUserGroup(String id,String busiDate){
        UserGroup byId = getById(id);
        super.baseMapper.deleteUserGroup(id);

        byId.setBusiDate(busiDate);

        List<TagCondition> tagConditions = JSON.parseArray(byId.getConditionJsonStr(), TagCondition.class);
        byId.setTagConditions(tagConditions);

        saveOrUpdate(byId);

        return byId;
    }

    /*
        select

         */
    public  String getSaveSql(UserGroup userGroup){
        Map<String, TagInfo> tagInfoMapWithCode = tagInfoService.getTagInfoMapWithCode();
        List<TagCondition> tagConditions = userGroup.getTagConditions();

        StringBuffer stringBuffer = new StringBuffer();
        for (TagCondition tagCondition : tagConditions) {
            String s = getsingleSql(userGroup.getBusiDate(), tagCondition, tagInfoMapWithCode);

            if(stringBuffer.length() == 0){
                stringBuffer.append("(").append(s).append(")");
            }else {
                stringBuffer.insert(0,"bitmapAnd(("+s+"),").append(")");
            }

        }



        return stringBuffer.toString();
    }

    //select groupBitmapMergeState(us) from user_tag_value_decimal
    //where tag_value < 100;
    public  String getsingleSql( String do_date, TagCondition tagCondition, Map<String, TagInfo> tagInfoMapWithCode) {
        String temp = "select groupBitmapMergeState(us) " +
                "from %s\n" +
                " where tag_value %s %s and dt = %s";
        String tagCode = tagCondition.getTagCode();
        TagInfo tagInfo = tagInfoMapWithCode.get(tagCode);
        String tagType = tagInfo.getTagValueType();
        String tableName ="";
        switch (tagType){
            case ConstCodes.TAG_VALUE_TYPE_LONG: tableName = "user_tag_value_long";break;
            case ConstCodes.TAG_VALUE_TYPE_DECIMAL: tableName = "user_tag_value_decimal";break;
            case ConstCodes.TAG_VALUE_TYPE_STRING: tableName = "user_tag_value_string";break;
            case ConstCodes.TAG_VALUE_TYPE_DATE: tableName = "user_tag_value_date";break;
        }

        String operator = tagCondition.getOperator();
        String operatorSql = getOperatorSql(operator);

        List<String> tagValues = tagCondition.getTagValues();
        if(tagType.equals(ConstCodes.TAG_VALUE_TYPE_DATE) || tagType.equals(ConstCodes.TAG_VALUE_TYPE_STRING)){
            tagValues = tagValues.stream().map(v -> "'" + v + "'").collect(Collectors.toList());
        }
        String tagValueSql = StringUtils.join(tagValues,",");

        if("in".equals(operatorSql) || "not in".equals(operatorSql)){
            tagValueSql = "(" + tagValueSql + ")";
        }

        String result = String.format(temp,tableName,operatorSql,tagValueSql,"'"+do_date+"'");

        return result;
    }

    public  String getOperatorSql(String operator){
        switch (operator) {
            case "eq" : return "=";
            case "neq" : return "<>";
            case "gte" : return ">=";
            case "lte" : return "<=";
            case "gt" : return ">";
            case "lt" : return "<";
            case "in" : return "in";
            case "nin" : return "not in";
        }
        throw new RuntimeException("没有匹配的操作符!");

    }

}
