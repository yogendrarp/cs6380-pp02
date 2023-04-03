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
    UPDATE_LEADER("UPDATE_LEADER"),
    MIN_EDGE("MIN_EDGE")
    ;

    String value;

    Messages(String val) {
        this.value = val;
    }
}
