package edu.buaa.act.jcsindex.local.jcssti.index.grid;

/**
 * Created by shmin at 2018/5/25 7:28
 **/
public class Grid {

    /*
     * 一、经纬度距离换算
     *
     * a）在纬度相等的情况下：
     *
     * 经度每隔0.00001度，距离相差约1米；
     *
     * 每隔0.0001度，距离相差约10米；
     *
     * 每隔0.001度，距离相差约100米；
     *
     * 每隔0.01度，距离相差约1000米；
     *
     * 每隔0.1度，距离相差约10000米。
     *
     * b）在经度相等的情况下：
     *
     * 纬度每隔0.00001度，距离相差约1.1米；
     *
     * 每隔0.0001度，距离相差约11米；
     *
     * 每隔0.001度，距离相差约111米；
     *
     * 每隔0.01度，距离相差约1113米；
     *
     * 每隔0.1度，距离相差约11132米。
     */
    // 北京地区
    // 最大经度116.848701941655， 最小经度115.542991693034
    // 最大纬度40.3485250469718， 最小纬度39.523733567851
    private float min_longitude;
    private float max_longitude;
    private float min_latitude;
    private float max_latitude;

    private int grid_size_x;
    private int grid_size_y;

    private float increament_longitude;
    private float increament_latitude;

    public Grid(float min_longitude, float min_latitude, float max_longitude, float max_latitude, int grid_size_x,
                int grid_size_y) {
        this.min_longitude = min_longitude;
        this.min_latitude = min_latitude;
        this.max_longitude = max_longitude;
        this.max_latitude = max_latitude;
        this.grid_size_x = grid_size_x;
        this.grid_size_y = grid_size_y;
        this.increament_longitude = (max_longitude - min_longitude) / grid_size_x;
        this.increament_latitude = (max_latitude - min_latitude) / grid_size_y;
    }

    /**
     * 根据给定经纬度坐标点获得所在的网格， 第一个网格(最左下的网格)坐标为[0,0]。<br/>
     * 每个网格不包括右边和上边边界，包括左边和下边边界，因此：点处在网格边界时得到右边边或上边或右上边的网格； 点处在整个大网格最右边或最上边时得到的相应坐标分量为GRID_SIZE_X或GRID_SIZE_Y；
     */
    public GridPosition getPosition(float longitude, float latitude) {
        return new GridPosition((int) Math.floor((longitude - min_longitude) / increament_longitude),
                (int) (Math.floor((latitude - min_latitude) / increament_latitude)));
    }

    /**
     * 根据经度获取grid横坐标
     */
    public int getX(float longitude) {
        return (int) Math.floor((longitude - min_longitude) / increament_longitude);
    }

    /**
     * 根据纬度获取grid纵坐标
     */
    public int getY(float latitude) {
        return (int) (Math.floor((latitude - min_latitude) / increament_latitude));
    }

    public float getMin_longitude() {
        return min_longitude;
    }

    public float getMax_longitude() {
        return max_longitude;
    }

    public float getMin_latitude() {
        return min_latitude;
    }

    public float getMax_latitude() {
        return max_latitude;
    }

    public int getGrid_size_x() {
        return grid_size_x;
    }

    public int getGrid_size_y() {
        return grid_size_y;
    }

    @Override
    public String toString() {
        return String.format(
                "longitude:[%3.13f,%3.13f], latitude:[%3.13f,%3.13f], grid:%d * %d, increment:[%3.13f,%3.13f]",
                min_longitude, max_longitude, min_latitude, max_latitude, grid_size_x, grid_size_y,
                increament_longitude, increament_latitude);
    }
}

class GridPosition {
    int x;
    int y;

    public GridPosition(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return "[" + x + "," + y + "]";
    }
}
