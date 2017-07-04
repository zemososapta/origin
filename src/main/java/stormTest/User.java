package stormTest;

public class User {
	private String userID;
	private int count;
	User(String employeeID,int count){
		this.userID=employeeID;
		this.count=count;
	}
	public String getUserID() {
		return userID;
	}
	public void setUserID(String employeeID) {
		this.userID = employeeID;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}

}
