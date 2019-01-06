package org.openpredict.exchange.rest.commands.admin;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public final class RestApiAddUser {

    private final long uid;

    @JsonCreator
    public RestApiAddUser(@JsonProperty("uid") long uid) {

        this.uid = uid;
    }

    @Override
    public String toString() {
        return "[ADDUSER " + uid + "]";
    }
}
