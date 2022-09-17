package com.atguigu.userprofile.mapper;

import com.atguigu.userprofile.bean.UserGroup;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.*;
import org.springframework.stereotype.Service;

import javax.ws.rs.DELETE;
import java.util.List;

/**
 * <p>
 *  Mapper 接口
 * </p>
 *
 * @author zhangchen
 * @since 2021-05-04
 */


@Mapper
@DS("mysql")
public interface UserGroupMapper extends BaseMapper<UserGroup> {

    @DS("ck")
    @Insert("insert into user_group select #{id},${sql}")
    void saveToCk(@Param("id") String id,@Param("sql") String sql);

    @DS("ck")
    @Select("select arrayJoin(bitmapToArray(${sql}))")
    List<String> getUserGroupFromCk(@Param("userGroup") UserGroup userGroup,@Param("sql") String sql);

    @DS("ck")
    @Select("select bitmapCardinality(${sql})")
    Long getUserGroupNum(@Param("userGroup") UserGroup userGroup,@Param("sql") String sql);

    @DS("ck")
    @Update("alter table user_group delete where user_group_id =#{id}")
    void deleteUserGroup(@Param("id") String id);
}
