import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class ElectionCompositeKey implements
WritableComparable<ElectionCompositeKey> {

int left;
String right;

public ElectionCompositeKey() {
}

public ElectionCompositeKey(int val1, String val2) {
left = val1;
right = val2;
}

@Override
public void readFields(DataInput in) throws IOException {
left = in.readInt();
right = in.readUTF();

}

@Override
public void write(DataOutput out) throws IOException {
out.writeInt(left);
out.writeUTF(right);
}

@Override
public int compareTo(ElectionCompositeKey otherkey) {
int ret = -1;
if (left > otherkey.left)
	ret = 1;
else if (left < otherkey.left)
	ret = -1;
else if (left == otherkey.left) {
	if (Integer.parseInt(right) > Integer.parseInt(otherkey.right))
		ret = 1;
	else if (Integer.parseInt(right) < Integer.parseInt(otherkey.right))
		ret = -1;
	else
		ret = 0;
}
return ret;
}

@Override
public int hashCode() {
final int prime = 31;
int result = 1;
result = prime * result + left;
result = prime * result + Integer.parseInt(right);
return result;
}

@Override
public boolean equals(Object obj) {
if (this == obj)
	return true;
if (obj == null)
	return false;
if (getClass() != obj.getClass())
	return false;
ElectionCompositeKey other = (ElectionCompositeKey) obj;
if (left != other.left)
	return false;
if (right != other.right)
	return false;
return true;
}

public String toString() {

return "" + left + "," + right;

}

// @Override
// public int compareTo(ElectionCompositeKey o) {
// // TODO Auto-generated method stub
// return 0;
// }
}
