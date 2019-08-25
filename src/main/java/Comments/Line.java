package Comments;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Line implements WritableComparable, DBWritable {

    private String scenery;
    private String comment;
    private int emotion;
    private int year;
    private int quarter;

//    public Line() {}

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeChars(this.scenery);
        dataOutput.writeChars(this.comment);
        dataOutput.writeInt(this.emotion);
        dataOutput.writeInt(this.year);
        dataOutput.writeInt(this.quarter);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.scenery = dataInput.readLine();
        this.comment = dataInput.readLine();
        this.emotion = dataInput.readInt();
        this.year = dataInput.readInt();
        this.quarter = dataInput.readInt();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, this.scenery);
        preparedStatement.setString(2, this.comment);
        preparedStatement.setInt(3, this.emotion);
        preparedStatement.setInt(4, this.year);
        preparedStatement.setInt(5, this.quarter);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.scenery = resultSet.getString(1);
        this.comment = resultSet.getString(2);
        this.emotion = resultSet.getInt(3);
        this.year = resultSet.getInt(4);
        this.quarter = resultSet.getInt(5);
    }

    public String getScenery() {
        return scenery;
    }

    public void setScenery(String scenery) {
        this.scenery = scenery;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public int getEmotion() {
        return emotion;
    }

    public void setEmotion(int emotion) {
        this.emotion = emotion;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getQuarter() {
        return quarter;
    }

    public void setQuarter(int quarter) {
        this.quarter = quarter;
    }
}
