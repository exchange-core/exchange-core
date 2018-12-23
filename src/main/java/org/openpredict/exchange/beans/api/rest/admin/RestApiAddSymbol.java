package org.openpredict.exchange.beans.api.rest.admin;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

@Getter
public class RestApiAddSymbol {

    private long symbolId;
    private String symbolName;

    @JsonCreator
    public RestApiAddSymbol(
            @JsonProperty("symbolName") String symbolName,
            @JsonProperty("symbolId") long symbolId) {

        this.symbolName = symbolName;
        this.symbolId = symbolId;
    }

    @Override
    public String toString() {
        return "[ADDSYMBOL " + symbolName + " " + symbolName + "]";
    }
}
