package bean;

public class MyEvent {

    private  int number;
    private  String name;
    private  String age;


    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "MyEvent{" +
                "number=" + number +
                ", name='" + name + '\'' +
                ", age='" + age + '\'' +
                '}';
    }
}
