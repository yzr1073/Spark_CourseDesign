package guat.lxy.bigdata.sprak_cousedesign3.entity;

import java.sql.Timestamp;

public class HotMovieRank {
    private Integer movieId;
    private Long clickCount;
    private Timestamp windowStart;
    private Timestamp windowEnd;
    private Timestamp createTime;

    // 无参构造（必须）
    public HotMovieRank() {}

    // 全参构造（可选）
    public HotMovieRank(Integer movieId, Long clickCount, Timestamp windowStart, Timestamp windowEnd, Timestamp createTime) {
        this.movieId = movieId;
        this.clickCount = clickCount;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.createTime = createTime;
    }

    // Getter 和 Setter
    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    public Timestamp getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Timestamp windowStart) {
        this.windowStart = windowStart;
    }

    public Timestamp getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Timestamp windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "HotMovieRank{" +
                "movieId=" + movieId +
                ", clickCount=" + clickCount +
                ", windowStart=" + windowStart +
                ", windowEnd=" + windowEnd +
                ", createTime=" + createTime +
                '}';
    }
}