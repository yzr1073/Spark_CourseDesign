package guat.lxy.bigdata.sprak_cousedesign3.controller;

import guat.lxy.bigdata.sprak_cousedesign3.entity.HotMovieTotal;
import guat.lxy.bigdata.sprak_cousedesign3.mapper.HotMovieTotalMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/hot")
public class HotMovieController {

    @Autowired
    private HotMovieTotalMapper hotMovieTotalMapper;

    @GetMapping("/rank")
    public List<HotMovieTotal> getRank() {
        return hotMovieTotalMapper.selectAllOrderByTotalDesc();
    }
}