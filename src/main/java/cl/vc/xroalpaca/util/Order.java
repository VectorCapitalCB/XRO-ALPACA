package cl.vc.xroalpaca.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Order {
    public String id;

    @JsonProperty("client_order_id")
    public String clientOrderId;

    @JsonProperty("created_at")
    public String createdAt;

    @JsonProperty("updated_at")
    public String updatedAt;

    @JsonProperty("submitted_at")
    public String submittedAt;

    @JsonProperty("filled_at")
    public String filledAt;

    @JsonProperty("expired_at")
    public String expiredAt;

    @JsonProperty("cancel_requested_at")
    public String cancelRequestedAt;

    @JsonProperty("canceled_at")
    public String canceledAt;

    @JsonProperty("failed_at")
    public String failedAt;

    @JsonProperty("replaced_at")
    public String replacedAt;

    @JsonProperty("replaced_by")
    public String replacedBy;

    @JsonProperty("replaces")
    public String replaces;

    @JsonProperty("asset_id")
    public String assetId;

    public String symbol;

    @JsonProperty("asset_class")
    public String assetClass;

    public String notional;
    public String qty;

    @JsonProperty("filled_qty")
    public String filledQty;

    @JsonProperty("filled_avg_price")
    public String filledAvgPrice;

    @JsonProperty("order_class")
    public String orderClass;

    @JsonProperty("order_type")
    public String orderType;

    public String type;
    public String side;

    @JsonProperty("position_intent")
    public String positionIntent;

    @JsonProperty("time_in_force")
    public String timeInForce;

    @JsonProperty("limit_price")
    public String limitPrice;

    @JsonProperty("stop_price")
    public String stopPrice;

    public String status;

    @JsonProperty("extended_hours")
    public boolean extendedHours;

    public String legs;
    @JsonProperty("trail_percent")
    public String trailPercent;

    @JsonProperty("trail_price")
    public String trailPrice;

    public String hwm;
    public String commission;

    @JsonProperty("expires_at")
    public String expiresAt;

    @JsonProperty("execution_id")
    public String executionId;
}