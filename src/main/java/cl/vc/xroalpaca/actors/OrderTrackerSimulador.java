package cl.vc.xroalpaca.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.xroalpaca.MainApp;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.TextFormat;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import quickfix.Message;
import quickfix.field.*;
import quickfix.fix44.OrderCancelReplaceRequest;

import java.time.LocalDateTime;
import java.util.HashMap;

@Slf4j
public class OrderTrackerSimulador extends AbstractActor {

    public static final String NAME = "order-tracker";
    private final RoutingMessage.OrderCancelReject.Builder rejectBuilder = RoutingMessage.OrderCancelReject.newBuilder();
    private Channel channel;
    private RoutingMessage.Order.Builder orderData;
    private int count = 0;
    private final HashMap<String, Message> waitingRequests = new HashMap<>();
    private String idactor;
    private Message lastRequest = new Message();
    private RoutingMessage.OrderReplaceRequest auxreplace;

    public OrderTrackerSimulador(Channel channel) {
        this.channel = channel;
    }

    public static Props props(Channel channel) {
        return Props.create(OrderTrackerSimulador.class, channel);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Channel.class, this::onChannell)
                .match(RoutingMessage.Order.class, this::onOrder)
                .match(RoutingMessage.NewOrderRequest.class, this::onNewOrder)
                .match(RoutingMessage.OrderReplaceRequest.class, this::replaceOrder)
                .match(RoutingMessage.OrderCancelRequest.class, this::cancelOrder)
                .build();
    }

    @Override
    public void preStart() {
        idactor = IDGenerator.getID();
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
            log.info("new chaner id {}  id actor {}", channel.id().toString(), idactor);

        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }


    private void sendMessage(com.google.protobuf.Message message) {
        channel.writeAndFlush(message);
    }

    private void notifyError(Exception e) {
        try {
            NotificationMessage.Notification notification = NotificationUtil.buildErrorNotification(orderData.getSecurityExchange(), e.getMessage(), NAME);
            sendMessage(notification);
        } catch (Exception exp) {
            log.error("Could not notify an exception: ", exp);
        }

    }

    private void onNewOrder(RoutingMessage.NewOrderRequest newOrderRequest) {

        RoutingMessage.Order.Builder msg = newOrderRequest.getOrder().toBuilder();
        this.orderData = msg;

        log.info("Assigned to new order: {}", msg.getId());

        try {

            if (msg.getPrice() == 0d && msg.getOrdType() == RoutingMessage.OrdType.LIMIT
                    && !msg.getSecurityExchange().name().contains("EXECUTION")) {
                msg.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                msg.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                msg.setText("No price");
                channel.writeAndFlush(msg.build());
                return;
            }

            if (msg.getSymbol().equals("SQM-B") && msg.getPrice() == 51000d && msg.getOrdType() == RoutingMessage.OrdType.LIMIT
                    && !msg.getSecurityExchange().name().contains("EXECUTION")) {
                msg.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                msg.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                msg.setText("No price");
                channel.writeAndFlush(msg.build());
                return;
            }

            String clordID = IDGenerator.getID(msg.getPrefixID());
            orderData.setClOrdId(clordID);


            if (!MainApp.getOrderMatcher().addOrder(msg.getSymbol(), msg.getSide(), msg.getPrice(), clordID)) {
                msg.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                msg.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                msg.setText("Order match prevention");
                channel.writeAndFlush(msg.build());
                return;
            }

            MainApp.getMessageEventBus().subscribe(getSelf(), orderData.getId());

            // SIMULACION

            msg.setExecType(RoutingMessage.ExecutionType.EXEC_NEW);
            msg.setOrdStatus(RoutingMessage.OrderStatus.NEW);
            msg.setText("simulator");
            msg.setExecId(IDGenerator.getID());
            msg.setTime(TimeGenerator.getTimeProto());
            msg.setLeaves(newOrderRequest.getOrder().getOrderQty());
            getSelf().tell(msg.build(), ActorRef.noSender());

            if (msg.getOrderQty() == 98d) {
                msg.setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED);
                msg.setOrdStatus(RoutingMessage.OrderStatus.CANCELED);
                msg.setText("simulator");
                msg.setExecId(IDGenerator.getID());
                msg.setTime(TimeGenerator.getTimeProto());
                msg.setLeaves(0d);
                getSelf().tell(msg.build(), ActorRef.noSender());

            } else if (msg.getOrderQty() == 99d) {
                msg.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                msg.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                msg.setText("simulator precio invalido");
                msg.setExecId(IDGenerator.getID());
                msg.setLeaves(0d);
                msg.setTime(TimeGenerator.getTimeProto());
                getSelf().tell(msg.build(), ActorRef.noSender());

            }

            if (msg.getOrderQty() == 100d) {
                RoutingMessage.Order.Builder trade = msg.clone();
                trade.setOrdStatus(RoutingMessage.OrderStatus.FILLED);
                trade.setExecType(RoutingMessage.ExecutionType.EXEC_TRADE);
                trade.setExecId(IDGenerator.getID());
                trade.setLastQty(trade.getOrderQty());
                trade.setLastPx(trade.getPrice());
                trade.setTime(TimeGenerator.getTimeProto());
                trade.setAvgPrice(trade.getPrice());
                trade.setText("simulator");
                trade.setCumQty(trade.getLastQty());
                trade.setFolio(IDGenerator.getID());
                trade.setContraBroker("041");
                trade.setLeaves(0d);
                trade.setContraTrader("041");
                getSelf().tell(trade.build(), ActorRef.noSender());
                return;

            } else if (msg.getOrderQty() == 90d || msg.getOrderQty() == 45d) {
                RoutingMessage.Order.Builder trade = msg.clone();
                trade.setOrdStatus(RoutingMessage.OrderStatus.FILLED);
                trade.setExecType(RoutingMessage.ExecutionType.EXEC_TRADE);
                trade.setExecId(IDGenerator.getID());
                trade.setLastQty(trade.getOrderQty());
                trade.setLastPx(trade.getPrice());
                trade.setTime(TimeGenerator.getTimeProto());
                trade.setAvgPrice(trade.getPrice());
                trade.setText("simulator");
                trade.setLeaves(0d);
                trade.setCumQty(trade.getLastQty());
                trade.setFolio(IDGenerator.getID());
                trade.setContraBroker("041");
                trade.setContraTrader("041");
                getSelf().tell(trade.build(), ActorRef.noSender());
                return;

            } else if (msg.getOrderQty() == 50d || msg.getOrderQty() == 30d) {
                RoutingMessage.Order.Builder trade = msg.clone();
                trade.setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED);
                trade.setExecType(RoutingMessage.ExecutionType.EXEC_TRADE);
                trade.setExecId(IDGenerator.getID());
                trade.setLastQty(27d);
                trade.setLastPx(trade.getPrice());
                trade.setTime(TimeGenerator.getTimeProto());
                trade.setAvgPrice(trade.getPrice());
                trade.setText("simulator");
                trade.setCumQty(27d);
                trade.setFolio(IDGenerator.getID());
                trade.setContraBroker("041");
                trade.setContraTrader("041");
                trade.setLeaves(23d);
                getSelf().tell(trade.build(), ActorRef.noSender());
                return;

            }

            if (msg.getOrderQty() == 200d) {

                RoutingMessage.Order.Builder trade1 = msg.clone();
                RoutingMessage.Order.Builder trade2 = msg.clone();
                RoutingMessage.Order.Builder trade3 = msg.clone();
                RoutingMessage.Order.Builder trade4 = msg.clone();

                trade1.setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED);
                trade1.setExecType(RoutingMessage.ExecutionType.EXEC_TRADE);
                trade1.setExecId(IDGenerator.getID());
                trade1.setLastQty(50d);
                trade1.setLastPx(trade1.getPrice());
                trade1.setTime(TimeGenerator.getTimeProto());
                trade1.setAvgPrice(trade1.getPrice());
                trade1.setText("simulator");
                trade1.setCumQty(50);
                trade1.setFolio(IDGenerator.getID());
                trade1.setContraBroker("041");
                trade1.setContraTrader("041");
                trade1.setLeaves(150);

                trade2.setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED);
                trade2.setExecType(RoutingMessage.ExecutionType.EXEC_TRADE);
                trade2.setExecId(IDGenerator.getID());
                trade2.setLastQty(50d);
                trade2.setLastPx(trade1.getPrice());
                trade2.setTime(TimeGenerator.getTimeProto());
                trade2.setAvgPrice(trade1.getPrice());
                trade2.setText("simulator");
                trade2.setCumQty(100d);
                trade2.setFolio(IDGenerator.getID());
                trade2.setContraBroker("041");
                trade2.setContraTrader("041");
                trade1.setLeaves(100d);

                trade3.setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED);
                trade3.setExecType(RoutingMessage.ExecutionType.EXEC_TRADE);
                trade3.setExecId(IDGenerator.getID());
                trade3.setLastQty(50d);
                trade3.setLastPx(trade1.getPrice());
                trade3.setTime(TimeGenerator.getTimeProto());
                trade3.setAvgPrice(trade1.getPrice());
                trade3.setText("simulator");
                trade3.setCumQty(150d);
                trade3.setFolio(IDGenerator.getID());
                trade3.setContraBroker("041");
                trade3.setContraTrader("041");
                trade3.setLeaves(50d);

                trade4.setOrdStatus(RoutingMessage.OrderStatus.FILLED);
                trade4.setExecType(RoutingMessage.ExecutionType.EXEC_TRADE);
                trade4.setExecId(IDGenerator.getID());
                trade4.setLastQty(50d);
                trade4.setLastPx(trade1.getPrice());
                trade4.setTime(TimeGenerator.getTimeProto());
                trade4.setAvgPrice(trade1.getPrice());
                trade4.setText("simulator");
                trade4.setCumQty(200d);
                trade4.setFolio(IDGenerator.getID());
                trade4.setContraBroker("041");
                trade4.setContraTrader("041");
                trade4.setLeaves(0d);


                getSelf().tell(trade1.build(), ActorRef.noSender());
                getSelf().tell(trade2.build(), ActorRef.noSender());
                getSelf().tell(trade3.build(), ActorRef.noSender());
                getSelf().tell(trade4.build(), ActorRef.noSender());

                return;

            }

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }

    private void replaceOrder(RoutingMessage.OrderReplaceRequest replaceRequest) {
        try {

            if (lastRequest == null) {
                rejectBuilder.setId(replaceRequest.getId());
                rejectBuilder.setText("Order pending");
                channel.writeAndFlush(rejectBuilder.build());
                return;
            }

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
            String newClOrdID = IDGenerator.getID(orderData.getPrefixID());

            Message message = (Message) lastRequest.clone();
            message.getHeader().setString(MsgType.FIELD, OrderCancelReplaceRequest.MSGTYPE);
            message.setField(new OrigClOrdID(message.getString(11)));
            message.setField(new ClOrdID(newClOrdID));

            message.setField(new OrderQty(replaceRequest.getQuantity()));

            if (message.isSetField(44)) {
                message.setField(new Price(replaceRequest.getPrice()));
            }

            message.setField(new TransactTime(LocalDateTime.now()));

            if (replaceRequest.getMaxFloor() > 0) {
                message.setField(new MaxFloor(replaceRequest.getMaxFloor()));
            }

            if (!MainApp.getOrderMatcher().addOrder(orderData.getSymbol(), orderData.getSide(), replaceRequest.getPrice(), newClOrdID)) {
                rejectBuilder.setId(replaceRequest.getId());
                rejectBuilder.setText("Order match prevention");
                channel.writeAndFlush(rejectBuilder.build());
                return;
            }

          //  waitingRequests.put(newClOrdID, message);
            MainApp.getSellSideManager().tell(new SellSideManager.SSReplace(message, getSelf(), orderData.build()), getSelf());

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }

    private void cancelOrder(RoutingMessage.OrderCancelRequest orderCancel) {
        try {

            orderData.setExecId(IDGenerator.getID());
            orderData.setOrdStatus(RoutingMessage.OrderStatus.CANCELED);
            orderData.setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED);
            orderData.setLeaves(0d);
            orderData.setText("simulator");
            orderData.setTime(TimeGenerator.getTimeProto());
            getSelf().tell(orderData.build(), ActorRef.noSender());

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        } finally {
            log.info("35=F {}", TextFormat.shortDebugString(orderCancel));
        }
    }


    private void onOrder(RoutingMessage.Order order) {
        try {


            orderData.setLeaves(order.getLeaves());
            orderData.setLastQty(order.getLastQty());
            orderData.setLastPx(order.getLastPx());
            orderData.setAvgPrice(order.getAvgPrice());
            orderData.setCumQty(order.getCumQty());

            channel.writeAndFlush(order);

            if (MainApp.getActorKafka() != null) {
                MainApp.getActorKafka().tell(order, ActorRef.noSender());
            }


        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }

}
