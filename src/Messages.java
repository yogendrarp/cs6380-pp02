import java.util.Locale;

public enum Messages {
    ENQUIRY("ENQUIRY"),
    TEST("TEST"),
    REJECT("REJECT"),
    REJECTNOTMWOE("REJECTNOTMWOE"),
    SAMECOMPONENT("SAMECOMPONENT"),
    COMPLETED("COMPLETED"),
    ACCEPT("ACCEPT"),
    ADD("ADD"),
    COMPLETE_CONTENDER("COMPLETE,CONTENDER"),
    COMPLETE_NONCONTENDER("COMPLETE,NONCONTENDER"),
    SEARCH("SEARCH"),
    ACK_MERGE("ACK_MERGE"),
    MIN_EDGE("MIN_EDGE"),
    CHANGE_LEADER("CHANGE_LEADER")
    ;

    String value;

    Messages(String val) {
        this.value = val;
    }
}
