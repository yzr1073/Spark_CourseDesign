package guat.lxy.bigdata.sprak_cousedesign3;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


// http://localhost:8080/hot/rank
// http://localhost:8080/hot-movie-dashboard.html


@SpringBootApplication
// 扫描所有mapper接口
@MapperScan("guat.lxy.bigdata.sprak_cousedesign3.mapper")
public class SprakCouseDesign3Application {
    public static void main(String[] args) {
        SpringApplication.run(SprakCouseDesign3Application.class, args);
    }
}