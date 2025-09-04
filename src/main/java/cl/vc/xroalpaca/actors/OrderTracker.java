package cl.vc.xroalpaca.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.xroalpaca.MainApp;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.TextFormat;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;
import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.Message;
import quickfix.field.*;
import quickfix.fix44.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;

@Slf4j
public class OrderTracker extends AbstractActor {

    public static final String NAME = "order-tracker";
    private final HashMap<String, Message> waitingRequests = new HashMap<>();
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
    private Channel channel;
    private RoutingMessage.Order.Builder orderData;
    private Message lastRequest = new Message();
    private double maxFloorAux = 0d;
    private String iceberpercent = "";
    private RoutingMessage.OrderReplaceRequest auxreplace;

    public OrderTracker(Channel channel) {
        this.channel = channel;
    }

    public static Props props(Channel channel) {
        return Props.create(OrderTracker.class, channel);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Channel.class, this::onChannell)
                .match(Message.class, this::onReceivedMessage)
                .match(RoutingMessage.Order.class, this::onOrder)
                .match(RoutingMessage.OrderCancelReject.class, this::onRejeceted)
                .match(RoutingMessage.NewOrderRequest.class, this::onNewOrder)
                .match(RoutingMessage.OrderReplaceRequest.class, this::replaceOrder)
                .match(RoutingMessage.OrderCancelRequest.class, this::cancelOrder)
                .build();
    }

    @Override
    public void preStart() {

        lastRequest.getHeader().setString(MsgType.FIELD, NewOrderSingle.MSGTYPE);

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

    private void onChannell(Channel msg) {
        try {
            this.channel = msg;
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

    private void onRejeceted(RoutingMessage.OrderCancelReject reject) {
        channel.writeAndFlush(reject);
    }


    private void onNewOrder(RoutingMessage.NewOrderRequest newOrderRequest) {

        RoutingMessage.Order.Builder msg = newOrderRequest.getOrder().toBuilder();
        this.orderData = msg;

        maxFloorAux = orderData.getMaxFloor();
        iceberpercent = orderData.getIcebergPercentage();

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
            orderData.setClOrdId(clordID);
            this.clOrdID.setValue(clordID);
            lastRequest.setField(this.clOrdID);

            orderQty.setValue(msg.getOrderQty());
            lastRequest.setField(orderQty);

            ordType.setValue(Integer.toString(msg.getOrdType().getNumber()).charAt(0));
            lastRequest.setField(ordType);

            double px = msg.getPrice();
            if (px != 0d && !newOrderRequest.getOrder().getOrdType().equals(RoutingMessage.OrdType.MARKET)) {
                price.setValue(px);
                lastRequest.setField(price);
            }

            side.setValue(Integer.toString(msg.getSide().getNumber()).charAt(0));
            lastRequest.setField(side);

            symbol.setValue(msg.getSymbol());
            lastRequest.setField(symbol);

            securityType.setValue(msg.getSecurityType().name());
            lastRequest.setField(securityType);

            timeInForce.setValue(Integer.toString(msg.getTif().getNumber()).charAt(0));
            lastRequest.setField(timeInForce);

            transactTime.setValue(LocalDateTime.now());
            lastRequest.setField(transactTime);

            settlType.setValue(Integer.toString(msg.getSettlType().getNumber()));
            lastRequest.setField(settlType);

            iceberpercent = msg.getIcebergPercentage();

            account.setValue(msg.getAccount());
            lastRequest.setField(account);

            if (msg.getMaxFloor() != 0) {
                maxFloor.setValue(msg.getMaxFloor());
                lastRequest.setField(maxFloor);
            }

            if (msg.getOrdType() == RoutingMessage.OrdType.MARKET_CLOSE) {

                ordType.setValue(OrdType.LIMIT_ON_CLOSE);
                lastRequest.setField(ordType);

                settlType.setValue(SettlType.T_2);
                lastRequest.setField(settlType);

                lastRequest.removeField(Price.FIELD);

                price2.setValue(msg.getPrice());
                lastRequest.setField(price2);

                agreementDesc.setValue(msg.getChkIndivisible() ? "I" : "D");
                lastRequest.setField(agreementDesc);
            }

            securityExchange.setValue(msg.getSecurityExchange().name());
            lastRequest.setField(securityExchange);
            lastRequest.setString(ClOrdLinkID.FIELD, orderData.getId());


            waitingRequests.put(newOrderRequest.getOrder().getId(), lastRequest);
            waitingRequests.put(clordID, lastRequest);
            MainApp.getMessageEventBus().subscribe(getSelf(), orderData.getId());

            MainApp.getSellSideManager().tell(new SellSideManager.SSNewOrder(lastRequest, msg, getSelf()), getSelf());

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

            waitingRequests.put(newClOrdID, message);
            MainApp.getSellSideManager().tell(new SellSideManager.SSReplace(message, getSelf(), orderData.build()), getSelf());

        } catch (Exception e) {
            notifyError(e);
            log.error(e.getMessage(), e);
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
            MainApp.getSellSideManager().tell(new SellSideManager.SSCancel(message, getSelf(), orderData.build()), getSelf());

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

            if (auxreplace != null) {
                orderData.setLimit(auxreplace.getLimit());
                orderData.setSpread(auxreplace.getSpread());
                orderData.setMaxFloor(auxreplace.getMaxFloor());
                orderData.setIcebergPercentage(auxreplace.getIcebergPercentage());
            }

            if (channel != null) {
                channel.writeAndFlush(orderData.build());
            }

            if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_NEW) || order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_REPLACED)) {
                if (waitingRequests.containsKey(order.getId())) {
                    lastRequest = waitingRequests.get(order.getId());
                    waitingRequests.remove(order.getId());
                }
            }

            if (order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_CANCELED) || order.getExecType().equals(RoutingMessage.ExecutionType.EXEC_REJECTED)) {
                if (waitingRequests.containsKey(clOrdID)) {
                    lastRequest = waitingRequests.remove(clOrdID);
                }
            }

            lastRequest.setString(ClOrdID.FIELD, order.getClOrdId());
            lastRequest.setString(OrderID.FIELD, order.getOrderID());
            lastRequest.setString(Side.FIELD, order.getSide().name());
            lastRequest.setString(ClOrdLinkID.FIELD, order.getId());

            if (!order.getOrigClOrdID().equals("")) {
                lastRequest.setString(OrigClOrdID.FIELD, order.getOrigClOrdID());
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

            if (auxreplace != null) {
                orderData.setLimit(auxreplace.getLimit());
                orderData.setSpread(auxreplace.getSpread());
                orderData.setMaxFloor(auxreplace.getMaxFloor());
                orderData.setIcebergPercentage(auxreplace.getIcebergPercentage());
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

            } else if (execType == ExecType.PENDING_NEW) {
                orderData.setExecType(RoutingMessage.ExecutionType.EXEC_NEW);

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

            if (report.isSetField(5463)) {
                orderData.setFolio(report.getString(5463));
            }

            orderData.setIcebergPercentage(iceberpercent);
            orderData.setMaxFloor(maxFloorAux);

            try {
                if (report.isSetField(NoContraBrokers.FIELD)) {
                    int noContraBrokers = report.getInt(NoContraBrokers.FIELD);

                    for (int i = 1; i <= noContraBrokers; i++) {
                        Group contraBrokerGroup = report.getGroup(i, NoContraBrokers.FIELD);
                        if (contraBrokerGroup.isSetField(ContraBroker.FIELD)) {
                            String contraBroker = contraBrokerGroup.getString(ContraBroker.FIELD);
                            orderData.setContraBroker(contraBroker);
                        }
                        if (contraBrokerGroup.isSetField(ContraTrader.FIELD)) {
                            String contraBroker = contraBrokerGroup.getString(ContraTrader.FIELD);
                            orderData.setContraTrader(contraBroker);
                        }
                    }
                }
            } catch (FieldNotFound e) {
                log.error(e.getMessage(), e);
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

            lastRequest.setString(OrderID.FIELD, report.getString(OrderID.FIELD));
            lastRequest.setString(ClOrdID.FIELD, report.getString(ClOrdID.FIELD));

            if (report.isSetField(OrigClOrdID.FIELD)) {
                lastRequest.setString(OrigClOrdID.FIELD, report.getString(OrigClOrdID.FIELD));
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
