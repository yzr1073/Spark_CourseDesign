package guat.lxy.bigdata.sprak_cousedesign3.mapper;

import guat.lxy.bigdata.sprak_cousedesign3.entity.HotMovieTotal;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface HotMovieTotalMapper {
    @Select("SELECT movie_id as movieId, total_click as totalClick, update_time as updateTime FROM hot_movie_total ORDER BY total_click DESC")
    List<HotMovieTotal> selectAllOrderByTotalDesc();
}