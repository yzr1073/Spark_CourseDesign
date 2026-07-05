package guat.lxy.bigdata.spark_cousedesign4.entity;

public class Recommendation {
    private String userId;
    private String movieId;
    private String movieTitle;
    private String predictRating;

    public Recommendation() {}

    public Recommendation(String userId, String movieId, String movieTitle, String predictRating) {
        this.userId = userId;
        this.movieId = movieId;
        this.movieTitle = movieTitle;
        this.predictRating = predictRating;
    }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getMovieId() { return movieId; }
    public void setMovieId(String movieId) { this.movieId = movieId; }
    public String getMovieTitle() { return movieTitle; }
    public void setMovieTitle(String movieTitle) { this.movieTitle = movieTitle; }
    public String getPredictRating() { return predictRating; }
    public void setPredictRating(String predictRating) { this.predictRating = predictRating; }
}