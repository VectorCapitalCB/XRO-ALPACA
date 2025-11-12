package cl.vc.xroalpaca.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.xroalpaca.MainApp;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderAlpacaTraker extends AbstractActor {

    private JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();
    private final RoutingMessage.OrderCancelReject.Builder rejectBuilder = RoutingMessage.OrderCancelReject.newBuilder();
    private Channel channel;
    private RoutingMessage.Order.Builder orderData;
    private RoutingMessage.OrderReplaceRequest auxreplace;

    public OrderAlpacaTraker(Channel channel) {
        this.channel = channel;
    }

    public static Props props(Channel channel) {
        return Props.create(OrderAlpacaTraker.class, channel);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Channel.class, this::onChannell)
                .match(RoutingMessage.Order.class, this::onOrder)
                .match(RoutingMessage.OrderCancelReject.class, this::onRejeceted)
                .match(RoutingMessage.NewOrderRequest.class, this::onNewOrder)
                .match(RoutingMessage.OrderReplaceRequest.class, this::replaceOrder)
                .match(RoutingMessage.OrderCancelRequest.class, this::cancelOrder)
                .build();
    }

    @Override
    public void preStart() {
    }

    @Override
    public void postStop() {
        if (orderData != null && orderData.getId() != null) {
            log.info("OrderTracker for {} terminated!", orderData.getId());
        }
    }

    private void onChannell(Channel msg) {
        try {
            this.channel = msg;
        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }


    private void onRejeceted(RoutingMessage.OrderCancelReject reject) {
        channel.writeAndFlush(reject);
    }


    private void onNewOrder(RoutingMessage.NewOrderRequest newOrderRequest) {

        try {

            RoutingMessage.Order.Builder msg = newOrderRequest.getOrder().toBuilder();
            this.orderData = msg;

            if (msg.getPrice() == 0d && msg.getOrdType() == RoutingMessage.OrdType.LIMIT
                    && !msg.getSecurityExchange().name().contains("EXECUTION")) {
                msg.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                msg.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                msg.setText("No price");
                channel.writeAndFlush(msg.build());
                return;
            }

            String clordID = IDGenerator.getID(msg.getPrefixID());
            orderData.setClOrdId(clordID);

            MainApp.getMessageEventBus().subscribe(getSelf(), orderData.getId());
            MainApp.getSellSideManager().tell(new SellSideManager.SSNewOrder(newOrderRequest, getSelf()), getSelf());

            log.info("Assigned to new order: {}", msg.getId());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void replaceOrder(RoutingMessage.OrderReplaceRequest replaceRequest) {
        try {

            if (replaceRequest.getPrice() == 0d && orderData.getOrdType() == RoutingMessage.OrdType.LIMIT
                    && !orderData.getSecurityExchange().name().contains("EXECUTION")) {
                rejectBuilder.setId(replaceRequest.getId());
                rejectBuilder.setText("No price");
                channel.writeAndFlush(rejectBuilder.build());
                return;
            }

            if (orderData.getPrice() == replaceRequest.getPrice()
                    && orderData.getOrderQty() == replaceRequest.getQuantity()
                    && replaceRequest.getMaxFloor() == orderData.getMaxFloor()) {
                orderData.setSpread(replaceRequest.getSpread());
                orderData.setLimit(replaceRequest.getLimit());
                channel.writeAndFlush(orderData.build());
                return;
            }

            auxreplace = replaceRequest;
            MainApp.getSellSideManager().tell(new SellSideManager.SSReplace(replaceRequest, getSelf(), orderData.build()), getSelf());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void cancelOrder(RoutingMessage.OrderCancelRequest orderCancel) {
        try {


            if (orderData.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED)
                    || orderData.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)
                    || orderData.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)) {

                rejectBuilder.setId(orderCancel.getId());
                rejectBuilder.setText("Order finished");
                channel.writeAndFlush(rejectBuilder.build());
                return;
            }

            MainApp.getSellSideManager().tell(new SellSideManager.SSCancel(orderCancel, getSelf(), orderData.build()), getSelf());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    private void onOrder(RoutingMessage.Order order) {
        try {

            orderData = order.toBuilder();

            if (auxreplace != null) {
                orderData.setLimit(auxreplace.getLimit());
                orderData.setSpread(auxreplace.getSpread());
                orderData.setMaxFloor(auxreplace.getMaxFloor());
                orderData.setIcebergPercentage(auxreplace.getIcebergPercentage());
            }

            if (channel != null) {
                channel.writeAndFlush(orderData.build());
            }

            MainApp.getActorKafka().tell(orderData.build(), ActorRef.noSender());
            log.info("orden enviada {}",printer.print(orderData));

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


/*
    private void handleCancelReplaceReject(Message report) {
        try {

            RoutingMessage.OrderCancelReject.Builder orderCancelReject = RoutingMessage.OrderCancelReject.newBuilder();

            if (report.isSetField(Text.FIELD)) {
                orderCancelReject.setText(report.getString(Text.FIELD));
            }

            orderCancelReject.setId(orderData.getId());

            RoutingMessage.OrderCancelReject reject = orderCancelReject.build();

            channel.writeAndFlush(reject);

            String clOrdID = report.getString(ClOrdID.FIELD);
            waitingRequests.remove(clOrdID);
            MainApp.getOrderMatcher().removeOrder(orderData.getSymbol(), orderData.getSide(), clOrdID);

            MainApp.getActorKafka().tell(reject, getSelf());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

 */
}
