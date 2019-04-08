package edu.buaa.act.jcsindex.local.bean;

/**
 * Created by shmin at 2018/7/5 23:33
 **/
/**
 * Currently hardcoded to 2 dimensions, but could be extended.
 */
public class ParaPoint implements java.io.Serializable {
    private static final long serialVersionUID = 1661664200123536273L;
    /**
     * The (x, y) coordinates of the point.
     */
    public float x, y;

    /**
     * Constructor.
     *
     * @param x
     *            The x coordinate of the point
     * @param y
     *            The y coordinate of the point
     */
    public ParaPoint(float x, float y) {
        this.x = x;
        this.y = y;
    }

    /**
     * Copy from another point into this one
     */
    public void set(ParaPoint other) {
        x = other.x;
        y = other.y;
    }

    /**
     * Print as a string in format "(x, y)"
     */
    @Override
    public String toString() {
        return "(" + x + ", " + y + ")";
    }

    /**
     * @return X coordinate rounded to an int
     */
    public int xInt() {
        return Math.round(x);
    }

    /**
     * @return Y coordinate rounded to an int
     */
    public int yInt() {
        return Math.round(y);
    }
}
