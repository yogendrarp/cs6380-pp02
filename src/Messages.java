import java.util.Locale;

public enum Messages {
    ENQUIRY("ENQUIRY"),
    TEST("TEST"),
    REJECT("REJECT"),
    ACCEPT("ACCEPT"),
    ADD("ADD"),
    COMPLETE_CONTENDER("COMPLETE,CONTENDER"),
    COMPLETE_NONCONTENDER("COMPLETE,NONCONTENDER"),
    SEARCH("SEARCH"),
    ACK_MERGE("ACK_MERGE"),
    MIN_EDGE("MIN_EDGE")
    ;

    String value;

    Messages(String val) {
        this.value = val;
    }
}
