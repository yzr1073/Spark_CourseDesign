package guat.lxy.bigdata.spark_cousedesign4.controller;

import guat.lxy.bigdata.spark_cousedesign4.entity.Recommendation;
import guat.lxy.bigdata.spark_cousedesign4.service.HBaseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/reco")
public class RecoController {

    @Autowired
    private HBaseService hBaseService;

    @GetMapping("/get")
    public List<Recommendation> getRecommendations(@RequestParam("userId") String userId) {
        return hBaseService.getRecommendationsForUser(userId);
    }
}