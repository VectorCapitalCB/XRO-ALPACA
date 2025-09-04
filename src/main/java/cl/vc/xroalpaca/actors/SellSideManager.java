package cl.vc.xroalpaca.actors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.xroalpaca.MainApp;
import cl.vc.xroalpaca.adapters.AlpacaAdapter;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import lombok.Getter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import quickfix.Message;
import quickfix.field.ClOrdID;
import quickfix.field.OrigClOrdID;
import quickfix.field.SecurityExchange;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class SellSideManager extends AbstractActor {

    public static final String NAME = "SellsideManager";
    @Getter
    private final HashMap<String, ActorRef> trackers = new HashMap<>();
    private final Map<RoutingMessage.SecurityExchangeRouting, NotificationMessage.Notification> statusConnections = new EnumMap<>(RoutingMessage.SecurityExchangeRouting.class);
    private HashMap<String, AlpacaAdapter> adapterMaps = new HashMap<>();
    private RoutingMessage.SecurityExchangeRouting securityExchange;
    private ActorRef clientManager;
    private String quickFixIniFile;

    private SellSideManager(ActorRef clientManager, RoutingMessage.SecurityExchangeRouting exchange, String quickFixIniFile) {
        try {

            this.quickFixIniFile = quickFixIniFile;
            this.clientManager = clientManager;
            this.securityExchange = exchange;
            createConnection(exchange);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static Props props(ActorRef clientManager, RoutingMessage.SecurityExchangeRouting exchange, String quickFixIniFile) {
        return Props.create(SellSideManager.class, clientManager, exchange, quickFixIniFile);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SSNewOrder.class, this::onNewOrderRequest)
                .match(SSReplace.class, this::onReplaceRequest)
                .match(SSCancel.class, this::onCancelOrderRequest)
                .match(Message.class, this::onMessage)
                .match(RoutingMessage.Order.class, this::onOrder)
                .match(RoutingMessage.OrderCancelReject.class, this::onrejected)
                .match(NotificationMessage.Notification.class, this::onNotificationConnect)
                .build();
    }


    private void createConnection(RoutingMessage.SecurityExchangeRouting exchange) {

        try {

            AlpacaAdapter adapter = new AlpacaAdapter(statusConnections, getSelf());
            adapterMaps.put(exchange.name(), adapter);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    @Override
    public void preStart() {
        try {

            adapterMaps.get(securityExchange.name()).startApplication();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void postStop() {
        try {
            adapterMaps.get(securityExchange.name()).stopApplication();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onOrder(RoutingMessage.Order msg) {
        try {

            if (!trackers.containsKey(msg.getId())) {
                ActorRef newActor = getContext().actorOf(OrderTracker.props(null));
                newActor.tell(msg, ActorRef.noSender());
                trackers.put(msg.getId(), newActor);
                //ETHubApp.getOrderTrackersUnknown().put(msg.getId(), newActor);

            } else {
                trackers.get(msg.getId()).tell(msg, getSelf());
            }

        } catch (Exception e) {
            sendErrorNotification(e.getMessage());
            log.error(e.getMessage(), e);
        }
    }

    private void onrejected(RoutingMessage.OrderCancelReject reject) {
        trackers.get(reject.getId()).tell(reject, getSelf());
    }


    private void sendCancelReject(Message fixMsg, Exception e) {
        onMessage(RejectsUtil.buildCancelOrderReject(fixMsg, e, NAME));
    }

    private void sendNewOrderReject(Message fixMsg, Exception e) {
        try {
            onMessage(RejectsUtil.buildNewOrderReject(fixMsg, e, NAME));
        } catch (Exception exc) {
            log.error("Could not send NewOrderReject: {}", exc.getMessage(), exc);
        }
    }

    private void sendErrorNotification(String error) {
        this.onNotificationConnect(NotificationUtil.buildErrorNotification(securityExchange, error, NAME));
    }

    private void onNewOrderRequest(SSNewOrder newOrder) {
        try {

            trackers.put(newOrder.getOrder().getId(), newOrder.getTracker());
            trackers.put(newOrder.getMessage().getString(ClOrdID.FIELD), newOrder.getTracker());

            if (!adapterMaps.containsKey(newOrder.getOrder().getSecurityExchange().name())) {
                createConnection(newOrder.getOrder().getSecurityExchange());
            }

            if (newOrder.getOrder().getSecurityExchange().name().contains("OFS")) {
                adapterMaps.get(RoutingMessage.SecurityExchangeRouting.XSGO.name()).putNewOrderSingle(newOrder.getMessage(), newOrder.getOrder().build());
                return;
            }

            adapterMaps.get(newOrder.getOrder().getSecurityExchange().name()).putNewOrderSingle(newOrder.getMessage(), newOrder.getOrder().build());


        } catch (Exception exc) {
            log.error("Error onNewOrderRequest", exc);
            sendNewOrderReject(newOrder.getMessage(), exc);
        }
    }

    private void onReplaceRequest(SSReplace msg) {
        try {

            trackers.put(msg.getMessage().getString(ClOrdID.FIELD), msg.getTracker());

            if (msg.getMessage().getString(SecurityExchange.FIELD).contains("OFS")) {
                adapterMaps.get(RoutingMessage.SecurityExchangeRouting.XSGO.name()).putOrderReplace(msg.getMessage(), msg.getOrder());
                return;
            }

            adapterMaps.get(msg.getMessage().getString(SecurityExchange.FIELD)).putOrderReplace(msg.getMessage(), msg.getOrder());

        } catch (Exception exc) {
            log.error("Error onCancelReplaceOrderRequest", exc);
            sendCancelReject(msg.getMessage(), exc);
        }
    }

    private void onCancelOrderRequest(SSCancel msg) {
        try {

            trackers.put(msg.getMessage().getString(ClOrdID.FIELD), msg.getTracker());


            if (msg.getMessage().getString(SecurityExchange.FIELD).contains("OFS")) {
                adapterMaps.get(RoutingMessage.SecurityExchangeRouting.XSGO.name()).putOrderReplace(msg.getMessage(), msg.getOrder());
                return;
            }


            adapterMaps.get(msg.getMessage().getString(SecurityExchange.FIELD)).putOrderCancel(msg.getMessage(), msg.getOrder());

        } catch (Exception exc) {
            log.error("Error onCancelOrderRequest: {}", exc.getMessage(), exc);
            sendCancelReject(msg.getMessage(), exc);
        }
    }

    private void onMessage(Message msg) {
        try {

            String clOrdID = msg.getString(ClOrdID.FIELD);

            if (trackers.containsKey(clOrdID)) {
                trackers.get(clOrdID).tell(msg, getSelf());

            } else if (msg.isSetField(OrigClOrdID.FIELD)) {
                String originlalcl = msg.getString(OrigClOrdID.FIELD);

                if (trackers.containsKey(originlalcl)) {
                    trackers.get(originlalcl).tell(msg, getSelf());
                }
            }

        } catch (Exception e) {
            sendErrorNotification(e.getMessage());
            log.error(e.getMessage(), e);
        }
    }

    private void onNotificationConnect(NotificationMessage.Notification msg) {
        try {

            clientManager.tell(msg, ActorRef.noSender());

        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }

    @Value
    public static class SSNewOrder {
        Message message;
        RoutingMessage.Order.Builder order;
        ActorRef tracker;
    }

    @Value
    public static class SSReplace {
        Message message;
        ActorRef tracker;
        RoutingMessage.Order order;
    }

    @Value
    public static class SSCancel {
        Message message;
        ActorRef tracker;
        RoutingMessage.Order order;
    }
}
