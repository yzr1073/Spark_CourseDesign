package guat.lxy.bigdata.sprak_cousedesign3.service.impl;

import guat.lxy.bigdata.sprak_cousedesign3.entity.HotMovieRank;
import guat.lxy.bigdata.sprak_cousedesign3.mapper.HotMovieRankMapper;
import guat.lxy.bigdata.sprak_cousedesign3.service.HotMovieRankService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class HotMovieRankServiceImpl implements HotMovieRankService {

    @Autowired
    private HotMovieRankMapper hotMovieRankMapper;

    @Override
    public List<HotMovieRank> getHotRankList() {
        // 可在此添加缓存或限制条数（例如返回前10）
        return hotMovieRankMapper.selectAllOrderByClickDesc();
    }
}