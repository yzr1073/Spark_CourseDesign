package guat.lxy.bigdata.sprak_cousedesign3.mapper;

import guat.lxy.bigdata.sprak_cousedesign3.entity.HotMovieRank;
import org.apache.ibatis.annotations.Mapper;
import java.util.List;

@Mapper
public interface HotMovieRankMapper {
    List<HotMovieRank> selectAllOrderByClickDesc();
}