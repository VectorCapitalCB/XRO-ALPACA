package cl.vc.xroalpaca.buyside;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.xroalpaca.MainApp;
import cl.vc.xroalpaca.actors.SellSideManager;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.TextFormat;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import quickfix.Message;
import quickfix.field.*;
import quickfix.fix44.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;

@Slf4j
public class OrderTrackerBuySide extends AbstractActor {
    public static final String NAME = "order-tracker";
    private final Channel channel;
    private final HashMap<String, Message> waitingRequests = new HashMap<>();
    // New Order Tags
    private final Message newOrderSingle = new Message();
    private final quickfix.fix50.ExecutionReport.NoStrategyParameters icebergPercentage = new quickfix.fix50.ExecutionReport.NoStrategyParameters();
    private final quickfix.fix50.ExecutionReport.NoStrategyParameters orderPrefix = new quickfix.fix50.ExecutionReport.NoStrategyParameters();
    private final quickfix.fix50.ExecutionReport.NoStrategyParameters limit = new quickfix.fix50.ExecutionReport.NoStrategyParameters();
    private final quickfix.fix50.ExecutionReport.NoStrategyParameters spread = new quickfix.fix50.ExecutionReport.NoStrategyParameters();
    private final quickfix.fix50.ExecutionReport.NoStrategyParameters strategy = new quickfix.fix50.ExecutionReport.NoStrategyParameters();
    private final OrderQty orderQty = new OrderQty();
    private final ClOrdID clOrdID = new ClOrdID();
    private final OrdType ordType = new OrdType();
    private final SettlType settlType = new SettlType();
    private final Price2 price2 = new Price2();
    private final Price price = new Price();
    private final Symbol symbol = new Symbol();
    private final SecurityType securityType = new SecurityType();
    private final TimeInForce timeInForce = new TimeInForce();
    private final TransactTime transactTime = new TransactTime();
    private final AgreementDesc agreementDesc = new AgreementDesc();
    private final Account account = new Account();
    private final Side side = new Side();
    private final SecurityExchange securityExchange = new SecurityExchange();
    private final MaxFloor maxFloor = new MaxFloor();
    private final Timestamp.Builder timestapBuilder = Timestamp.newBuilder();
    private final RoutingMessage.OrderCancelReject.Builder rejectBuilder = RoutingMessage.OrderCancelReject.newBuilder();
    private JsonFormat.Printer printer;
    private RoutingMessage.Order.Builder orderData;
    private Message lastRequest = null;

    public OrderTrackerBuySide(Channel channel) {
        this.channel = channel;
    }

    public static Props props(Channel channel) {
        return Props.create(OrderTrackerBuySide.class, channel);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Message.class, this::onReceivedMessage)
                .match(RoutingMessage.Order.class, this::onOrder)
                .match(RoutingMessage.NewOrderRequest.class, this::onNewOrder)
                .match(RoutingMessage.OrderReplaceRequest.class, this::replaceOrder)
                .match(RoutingMessage.OrderCancelRequest.class, this::cancelOrder)
                .build();
    }

    @Override
    public void preStart() {

        newOrderSingle.getHeader().setString(MsgType.FIELD, NewOrderSingle.MSGTYPE);

        icebergPercentage.set(new StrategyParameterName("IcebergPercentage"));
        icebergPercentage.set(new StrategyParameterType(11));

        orderPrefix.set(new StrategyParameterName("OrderPrefix"));
        orderPrefix.set(new StrategyParameterType(14));

        limit.set(new StrategyParameterName("Limit"));
        limit.set(new StrategyParameterType(11));

        spread.set(new StrategyParameterName("Spread"));
        spread.set(new StrategyParameterType(11));

        strategy.set(new StrategyParameterName("Strategy"));
        strategy.set(new StrategyParameterType(14));

        this.printer = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();
    }

    @Override
    public void postStop() {
        if (orderData != null && orderData.getId() != null) {
            log.info("OrderTracker for {} terminated!", orderData.getId());
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

            String clordID = IDGenerator.getID(msg.getPrefixID());

            this.clOrdID.setValue(clordID);
            newOrderSingle.setField(this.clOrdID);

            orderQty.setValue(msg.getOrderQty());
            newOrderSingle.setField(orderQty);

            ordType.setValue(Integer.toString(msg.getOrdType().getNumber()).charAt(0));
            newOrderSingle.setField(ordType);

            double px = msg.getPrice();
            if (px != 0d) {
                price.setValue(px);
                newOrderSingle.setField(price);
            }

            side.setValue(Integer.toString(msg.getSide().getNumber()).charAt(0));
            newOrderSingle.setField(side);

            symbol.setValue(msg.getSymbol());
            newOrderSingle.setField(symbol);

            securityType.setValue(msg.getSecurityType().name());
            newOrderSingle.setField(securityType);

            timeInForce.setValue(Integer.toString(msg.getTif().getNumber()).charAt(0));
            newOrderSingle.setField(timeInForce);

            transactTime.setValue(LocalDateTime.now());
            newOrderSingle.setField(transactTime);

            settlType.setValue(Integer.toString(msg.getSettlType().getNumber()));
            newOrderSingle.setField(settlType);

            account.setValue(msg.getAccount());
            newOrderSingle.setField(account);

            if (msg.getMaxFloor() != 0) {
                maxFloor.setValue(msg.getMaxFloor());
                newOrderSingle.setField(maxFloor);
            }

            if (msg.getOrdType() == RoutingMessage.OrdType.MARKET_CLOSE) {

                ordType.setValue(OrdType.LIMIT_ON_CLOSE);
                newOrderSingle.setField(ordType);

                settlType.setValue(SettlType.T_2);
                newOrderSingle.setField(settlType);

                newOrderSingle.removeField(Price.FIELD);

                price2.setValue(msg.getPrice());
                newOrderSingle.setField(price2);

                agreementDesc.setValue(msg.getChkIndivisible() ? "I" : "D");
                newOrderSingle.setField(agreementDesc);
            }

            securityExchange.setValue(msg.getSecurityExchange().name());
            newOrderSingle.setField(securityExchange);
            newOrderSingle.setString(ClOrdLinkID.FIELD, orderData.getId());


            if (!MainApp.getOrderMatcher().addOrder(msg.getSymbol(), msg.getSide(), msg.getPrice(), clordID)) {
                msg.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                msg.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                msg.setText("Order match prevention");
                channel.writeAndFlush(msg.build());
                return;
            }

            MainApp.getSellSideManager().tell(new SellSideManager.SSNewOrder(newOrderSingle, msg, getSelf()), getSelf());

            waitingRequests.put(newOrderRequest.getOrder().getId(), newOrderSingle);
            waitingRequests.put(clordID, newOrderSingle);
            MainApp.getMessageEventBus().subscribe(getSelf(), orderData.getId());

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        } finally {
            log.info("35=D {}", TextFormat.shortDebugString(msg));
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

            if (orderData.getMaxFloor() != 0) {
                double icebergPct = orderData.getMaxFloor() / 100;
                int floor = (int) Math.ceil(Math.min(replaceRequest.getQuantity() - orderData.getCumQty(), replaceRequest.getQuantity() * icebergPct));
                message.setField(new MaxFloor(floor));
            }

            if (!MainApp.getOrderMatcher().addOrder(orderData.getSymbol(), orderData.getSide(), replaceRequest.getPrice(), newClOrdID)) {
                rejectBuilder.setId(replaceRequest.getId());
                rejectBuilder.setText("Order match prevention");
                channel.writeAndFlush(rejectBuilder.build());
                return;
            }
            waitingRequests.put(newClOrdID, message);
            //   ETHubApp.getSellSideManager().tell(new SellSideManager.SSReplace(message, getSelf()), getSelf());

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        } finally {
            log.info("35=G {}", TextFormat.shortDebugString(replaceRequest));
        }
    }

    private void cancelOrder(RoutingMessage.OrderCancelRequest orderCancel) {
        try {
            if (lastRequest == null) {
                rejectBuilder.setId(orderCancel.getId());
                rejectBuilder.setText("Order pending");
                channel.writeAndFlush(rejectBuilder.build());
                return;
            }
            Message message = (Message) lastRequest.clone();

            if (orderData.getOrdStatus().equals(RoutingMessage.OrderStatus.FILLED)
                    || orderData.getOrdStatus().equals(RoutingMessage.OrderStatus.CANCELED)
                    || orderData.getOrdStatus().equals(RoutingMessage.OrderStatus.REJECTED)) {

                rejectBuilder.setId(orderCancel.getId());
                rejectBuilder.setText("Order finished");
                channel.writeAndFlush(rejectBuilder.build());
                return;
            }

            message.getHeader().setString(MsgType.FIELD, OrderCancelRequest.MSGTYPE);
            message.setField(new OrigClOrdID(message.getString(11)));

            String newClOrdID = IDGenerator.getID(orderData.getPrefixID());

            message.setField(new ClOrdID(newClOrdID));

            message.removeField(OrdType.FIELD);
            message.removeField(Price.FIELD);
            message.removeField(OrdType.FIELD);
            message.removeField(TimeInForce.FIELD);
            message.removeField(SettlType.FIELD);
            message.removeField(MaxFloor.FIELD);

            waitingRequests.put(newClOrdID, message);
            //    ETHubApp.getSellSideManager().tell(new SellSideManager.SSCancel(message, getSelf()), getSelf());

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        } finally {
            log.info("35=F {}", TextFormat.shortDebugString(orderCancel));
        }
    }


    private void onOrder(RoutingMessage.Order order) {
        try {

            orderData = order.toBuilder();

            channel.writeAndFlush(orderData.build());

            if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_NEW)
                    || order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_REPLACED)) {

                if (waitingRequests.containsKey(order.getId())) {
                    lastRequest = waitingRequests.get(order.getId());
                    waitingRequests.remove(order.getId());
                }

                MainApp.getOrderMatcher().setOrder(orderData.getSymbol(), orderData.getSide(), orderData.getPrice(), orderData.getId());
            }

            if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_CANCELED) || order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_REJECTED)) {
                if (waitingRequests.containsKey(clOrdID)) {
                    lastRequest = waitingRequests.remove(clOrdID);
                }
                MainApp.getOrderMatcher().removeOrder(orderData.getSymbol(), orderData.getSide(), orderData.getId());
            }

            if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE)) {
                MainApp.getOrderMatcher().removeOrder(orderData.getSymbol(), orderData.getSide(), orderData.getId());
            }


            newOrderSingle.setString(ClOrdID.FIELD, order.getClOrdId());
            newOrderSingle.setString(OrderID.FIELD, order.getOrderID());
            newOrderSingle.setString(Side.FIELD, order.getSide().name());
            newOrderSingle.setString(ClOrdLinkID.FIELD, order.getId());

            if (!order.getOrigClOrdID().equals("")) {
                newOrderSingle.setString(OrigClOrdID.FIELD, order.getOrigClOrdID());
            }

            MainApp.getActorKafka().tell(orderData.build(), ActorRef.noSender());

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }

    private void onReceivedMessage(Message receivedMessage) {
        try {
            String msgType = receivedMessage.getHeader().getString(MsgType.FIELD);

            if (msgType.equals(ExecutionReport.MSGTYPE)) {
                handleReport(receivedMessage);
            } else if (msgType.equals(OrderCancelReject.MSGTYPE)) {
                handleCancelReplaceReject(receivedMessage);
            }

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }

    private void handleReport(Message report) {
        try {

            orderData.setOrderID(report.getString(OrderID.FIELD));
            orderData.setClOrdId(report.getString(ClOrdID.FIELD));
            orderData.setExecId(report.getString(ExecID.FIELD));

            if (report.isSetField(Price.FIELD)) {
                orderData.setPrice((report.getDouble(Price.FIELD)));
            }
            orderData.setOrderQty(report.getDouble(OrderQty.FIELD));
            orderData.setLeaves(report.getDouble(LeavesQty.FIELD));

            char execType = report.getChar(ExecType.FIELD);

            if (execType == ExecType.PARTIAL_FILL || execType == ExecType.FILL || execType == ExecType.TRADE) {

                orderData.setExecType(RoutingMessage.ExecutionType.EXEC_TRADE);
                orderData.setLastPx(report.getDouble(LastPx.FIELD));
                orderData.setAvgPrice(report.getDouble(AvgPx.FIELD));
                orderData.setLastQty(report.getDouble(LastQty.FIELD));
                orderData.setCumQty(report.getDouble(CumQty.FIELD));

            } else if (execType == ExecType.PENDING_REPLACE) {
                orderData.setExecType(RoutingMessage.ExecutionType.EXEC_PENDING_REPLACE);
            } else if (execType == ExecType.PENDING_CANCEL) {
                orderData.setExecType(RoutingMessage.ExecutionType.EXEC_PENDING_CANCEL);
            } else {
                orderData.setExecType(RoutingMessage.ExecutionType.forNumber(report.getInt(ExecType.FIELD)));
            }

            char status = report.getChar(OrdStatus.FIELD);

            if (status == OrdStatus.PENDING_NEW) {
                orderData.setOrdStatus(RoutingMessage.OrderStatus.PENDING_NEW);
            } else if (status == OrdStatus.EXPIRED) {
                orderData.setOrdStatus(RoutingMessage.OrderStatus.EXPIRED);
            } else if (status == OrdStatus.CALCULATED) {
                orderData.setOrdStatus(RoutingMessage.OrderStatus.CALCULATED);
            } else if (report.getChar(OrdStatus.FIELD) == OrdStatus.PENDING_REPLACE) {
                orderData.setOrdStatus(RoutingMessage.OrderStatus.PENDING_REPLACE);
            } else {
                orderData.setOrdStatus(RoutingMessage.OrderStatus.forNumber(report.getInt(OrdStatus.FIELD)));
            }

            if (orderData.getOrdType() == RoutingMessage.OrdType.MARKET_CLOSE && report.isSetField(Price2.FIELD)) {
                orderData.setPrice(report.getDouble(Price2.FIELD));
            }

            LocalDateTime localDateTime = report.getUtcTimeStamp(TransactTime.FIELD);
            Instant instant = localDateTime.toInstant(ZoneOffset.UTC);
            orderData.setTime(timestapBuilder.setSeconds(instant.getEpochSecond()).setNanos(instant.getNano()).build());

            if (report.isSetField(Text.FIELD)) {
                orderData.setText(report.getString(Text.FIELD));
            }

            channel.writeAndFlush(orderData.build());

            log.info(printer.print(orderData));

            String clOrdID = report.getString(ClOrdID.FIELD);
            if (report.getChar(ExecType.FIELD) == ExecType.NEW
                    || report.getChar(ExecType.FIELD) == ExecType.REPLACED) {

                if (waitingRequests.containsKey(clOrdID)) {
                    lastRequest = waitingRequests.remove(clOrdID);
                }

                MainApp.getOrderMatcher().setOrder(orderData.getSymbol(), orderData.getSide(), orderData.getPrice(), orderData.getId());
                MainApp.getOrderMatcher().removeOrder(orderData.getSymbol(), orderData.getSide(), clOrdID);
            }

            if (report.getChar(ExecType.FIELD) == ExecType.CANCELED || report.getChar(ExecType.FIELD) == ExecType.REJECTED) {
                if (waitingRequests.containsKey(clOrdID)) {
                    lastRequest = waitingRequests.remove(clOrdID);
                }
                MainApp.getOrderMatcher().removeOrder(orderData.getSymbol(), orderData.getSide(), clOrdID);
                MainApp.getOrderMatcher().removeOrder(orderData.getSymbol(), orderData.getSide(), orderData.getId());
            }

            if (report.getChar(OrdStatus.FIELD) == OrdStatus.EXPIRED
                    || report.getChar(OrdStatus.FIELD) == OrdStatus.FILLED) {
                MainApp.getOrderMatcher().removeOrder(orderData.getSymbol(), orderData.getSide(), orderData.getId());
            }

            newOrderSingle.setString(OrderID.FIELD, report.getString(OrderID.FIELD));
            newOrderSingle.setString(ClOrdID.FIELD, report.getString(ClOrdID.FIELD));

            if (report.isSetField(OrigClOrdID.FIELD)) {
                newOrderSingle.setString(OrigClOrdID.FIELD, report.getString(OrigClOrdID.FIELD));
            }


            MainApp.getActorKafka().tell(orderData.build(), ActorRef.noSender());

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }

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
            notifyError(e);
            log.error(e.getMessage(), e);
        }
    }
}
