package guat.lxy.bigdata.sprak_cousedesign3.entity;

import java.sql.Timestamp;

public class HotMovieTotal {
    private Integer movieId;
    private Long totalClick;
    private Timestamp updateTime;

    public Integer getMovieId() { return movieId; }
    public void setMovieId(Integer movieId) { this.movieId = movieId; }
    public Long getTotalClick() { return totalClick; }
    public void setTotalClick(Long totalClick) { this.totalClick = totalClick; }
    public Timestamp getUpdateTime() { return updateTime; }
    public void setUpdateTime(Timestamp updateTime) { this.updateTime = updateTime; }
}