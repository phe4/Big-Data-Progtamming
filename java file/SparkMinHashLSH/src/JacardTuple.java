import java.io.Serializable;
public class JacardTuple implements Serializable {
/**
*
*/
private static final long serialVersionUID = 44545;
private double count;
private double union;
public JacardTuple(double count, double union) {
super();
this.count = count;
this.union = union;
}
public JacardTuple() {
super();
// TODO Auto-generated constructor stub
}
public double getCount() {
return count;
}
public void setCount(double count) {
this.count = count;
}
public double getUnion() {
return union;
}
public void setUnion(double union) {
this.union = union;
}
public static long getSerialversionuid() {
return serialVersionUID;
}
@Override
public String toString() {
return String
.valueOf(union/count);
}
}