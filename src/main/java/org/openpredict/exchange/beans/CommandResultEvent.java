package org.openpredict.exchange.beans;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CommandResultEvent {
    public CommandResultEventType eventType;
    public int symbol;
    public long orderId;
    public long uid;
    public long timestamp;
}
