package cl.vc.xroalpaca.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class OrderEvent {
    @JsonProperty("account_id")
    public String accountId;

    @JsonProperty("at")
    public String at;

    @JsonProperty("event_id")
    public String eventId;

    @JsonProperty("event")
    public String event;

    @JsonProperty("timestamp")
    public String timestamp;

    @JsonProperty("order")
    public Order order;

}