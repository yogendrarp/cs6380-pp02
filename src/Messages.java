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
    ASKPARENT("ASKPARENT"),
    SEARCH_REJECT("SEARCH_REJECT"),
    REJECT_BY_PARENT("REJECT_BY_PARENT"),
    UPDATE_LEADER("UPDATE_LEADER"),
    MIN_EDGE("MIN_EDGE")
    ;

    String value;

    Messages(String val) {
        this.value = val;
    }
}
