public class Dummy {
    public static void main(String[] args) {
        int myuid = 600;
        String msg = "TEST,300,1,202,203,204,600";
        String[] splist = msg.split(",");
        int k = -1;
        for (int i = 0; i < splist.length; i++) {
            if (splist[i].equals(myuid + "")) {
                k = i + 1;
            }
        }
        if (k == splist.length) {
            System.out.println("I am the last one, send my parent");
            //Append my parent and send to it
        }
        else{
            System.out.println(splist[k]);
        }
    }
}
