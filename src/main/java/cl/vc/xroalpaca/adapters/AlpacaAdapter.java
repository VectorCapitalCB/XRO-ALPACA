package cl.vc.xroalpaca.adapters;

import akka.actor.ActorRef;
import cl.vc.xroalpaca.MainApp;
import cl.vc.xroalpaca.util.OrderEvent;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import okio.BufferedSource;
import org.json.JSONObject;


import java.io.IOException;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;


@lombok.extern.slf4j.Slf4j
public class AlpacaAdapter implements IQuickFixAdapter {

    private static final String BASE_URL = "https://broker-api.sandbox.alpaca.markets/v1/accounts";
    private static final String SSE_URL = "https://broker-api.sandbox.alpaca.markets/v1/events/orders";

    private Map<RoutingMessage.SecurityExchangeRouting, NotificationMessage.Notification> statusConnections;
    private ActorRef sellsideManager;
    private HashMap<String, RoutingMessage.Order.Builder> mapsOrders = new HashMap<>();
    private String key;
    private String secret;

    public AlpacaAdapter(Map<RoutingMessage.SecurityExchangeRouting, NotificationMessage.Notification> statusConnections, ActorRef actorRef) {

        try {

            sellsideManager = actorRef;
            this.statusConnections = statusConnections;

            key = MainApp.getProperties().getProperty("alpaca.key").trim();
            secret = MainApp.getProperties().getProperty("alpaca.secreto").trim();

            //getCLientID();
            // connect();

            cancelAllOrders("633b1575-2996-4d89-83f7-b8aa89ae2031");
            cancelAllOrders("7824c546-4170-487f-a189-56c1efa095de");
            cancelAllOrders("7824c546-4170-487f-a189-56c1efa095de");

            seeEvent();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    public void seeEvent() {
        new Thread(() -> {
            while (true) {

                OkHttpClient client = new OkHttpClient.Builder()
                        .readTimeout(Duration.ofMinutes(5))
                        .pingInterval(Duration.ofSeconds(5)) // mantiene viva la conexión
                        .retryOnConnectionFailure(true)
                        .build();

                String credentials = key + ":" + secret;
                String encodedAuth = Base64.getEncoder().encodeToString(credentials.getBytes());

                Request request = new Request.Builder()
                        .url("https://broker-api.sandbox.alpaca.markets/v2/events/trades")
                        .header("Authorization", "Basic " + encodedAuth)
                        .addHeader("Accept", "text/event-stream")
                        .build();

                log.info("🔁 Intentando conectar al SSE...");

                try (Response response = client.newCall(request).execute()) {
                    if (!response.isSuccessful()) {
                        log.error("❌ Error HTTP: " + response.code());
                        reconnectDelay();
                        continue;
                    }

                    log.info("✅ Conectado al SSE. Esperando eventos...");

                    try (BufferedSource source = response.body().source()) {
                        while (!source.exhausted()) {
                            String line = source.readUtf8LineStrict();

                            if (line.startsWith("data:")) {
                                String jsonEvent = line.substring(5).trim();

                                ObjectMapper mapper = new ObjectMapper();
                                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                                OrderEvent event = mapper.readValue(jsonEvent, OrderEvent.class);

                                if (event.getOrder().getStatus().contains("pending")) {
                                    log.info("❌ mensaje retornado {}", event.getOrder());
                                    continue; // no cortar el bucle
                                }

                                log.info(event.getEvent());

                                if ("new".equals(event.getOrder().getStatus())) {

                                    RoutingMessage.Order.Builder oBuilder = mapsOrders.get(event.getOrder().getClientOrderId());
                                    oBuilder.setOrderID(event.getOrder().getId());
                                    oBuilder.setPrice(Double.parseDouble(event.getOrder().getLimitPrice()));
                                    oBuilder.setOrderQty(Double.parseDouble(event.getOrder().getQty()));
                                    oBuilder.setOrdStatus(RoutingMessage.OrderStatus.NEW);
                                    oBuilder.setExecType(RoutingMessage.ExecutionType.EXEC_NEW);
                                    oBuilder.setExecId(IDGenerator.getID());
                                    oBuilder.setTime(TimeGenerator.getTimeProto());
                                    mapsOrders.put(oBuilder.getId(), oBuilder);
                                    sellsideManager.tell(oBuilder.build(), ActorRef.noSender());

                                } else if ("canceled".equals(event.getOrder().getStatus())) {

                                    RoutingMessage.Order.Builder oBuilder = mapsOrders.get(event.getOrder().getClientOrderId());
                                    oBuilder.setOrdStatus(RoutingMessage.OrderStatus.CANCELED);
                                    oBuilder.setExecType(RoutingMessage.ExecutionType.EXEC_CANCELED);
                                    oBuilder.setLeaves(0d);
                                    oBuilder.setExecId(IDGenerator.getID());
                                    oBuilder.setTime(TimeGenerator.getTimeProto());
                                    mapsOrders.put(oBuilder.getId(), oBuilder);
                                    sellsideManager.tell(oBuilder.build(), ActorRef.noSender());

                                } else if ("filled".equals(event.getOrder().getStatus())) {

                                    RoutingMessage.Order.Builder oBuilder = mapsOrders.get(event.getOrder().getClientOrderId()).clone();
                                    oBuilder.setOrdStatus(RoutingMessage.OrderStatus.FILLED);
                                    oBuilder.setExecType(RoutingMessage.ExecutionType.EXEC_TRADE);
                                    oBuilder.setLeaves(0d);
                                    oBuilder.setCumQty(oBuilder.getOrderQty());
                                    oBuilder.setAvgPrice(Double.parseDouble(event.getOrder().getFilledAvgPrice()));
                                    oBuilder.setLastPx(Double.parseDouble(event.getOrder().getFilledAvgPrice()));
                                    oBuilder.setLastQty(Double.parseDouble(event.getOrder().getFilledQty()));
                                    oBuilder.setExecId(IDGenerator.getID());
                                    oBuilder.setTime(TimeGenerator.getTimeProto());

                                    sellsideManager.tell(oBuilder.build(), ActorRef.noSender());

                                } else if ("replaced".equals(event.getOrder().getStatus())) {

                                    RoutingMessage.Order.Builder oBuilder = mapsOrders.get(event.getOrder().getClientOrderId());

                                    if (!event.getOrder().getReplacedBy().isEmpty()) {
                                        oBuilder.setOrderID(event.getOrder().getReplacedBy());
                                        log.info("actualizamos order_id {}", event.getOrder().getReplacedBy());
                                    }

                                    oBuilder.setPrice(Double.parseDouble(event.getOrder().getLimitPrice()));
                                    oBuilder.setOrderQty(Double.parseDouble(event.getOrder().getQty()));
                                    oBuilder.setOrdStatus(RoutingMessage.OrderStatus.REPLACED);
                                    oBuilder.setExecType(RoutingMessage.ExecutionType.EXEC_REPLACED);
                                    oBuilder.setExecId(IDGenerator.getID());
                                    oBuilder.setTime(TimeGenerator.getTimeProto());
                                    oBuilder.setCumQty(Double.parseDouble(event.getOrder().getFilledQty()));

                                    if (oBuilder.getCumQty() > 0d && oBuilder.getCumQty() < oBuilder.getOrderQty()) {
                                        oBuilder.setOrdStatus(RoutingMessage.OrderStatus.PARTIALLY_FILLED);
                                    }

                                    mapsOrders.put(oBuilder.getId(), oBuilder);
                                    sellsideManager.tell(oBuilder.build(), ActorRef.noSender());

                                } else {
                                    log.info("mensaej no procesao {}", event.getEvent());
                                }

                                log.info("📨 Evento recibido: {}\n", jsonEvent);

                            }
                        }
                    }

                } catch (Exception e) {
                    log.error("❌ Error SSE: " + e.getMessage());
                }

                reconnectDelay();
            }
        }).start();
    }

    public void cancelAllOrders(String account) {
        try {
            OkHttpClient client = new OkHttpClient();

            String accountId = account;
            String credentials = key + ":" + secret;
            String encodedAuth = Base64.getEncoder().encodeToString(credentials.getBytes());

            String url = String.format(
                    "https://broker-api.sandbox.alpaca.markets/v1/trading/accounts/%s/orders",
                    accountId
            );

            Request request = new Request.Builder()
                    .url(url)
                    .delete()
                    .addHeader("Authorization", "Basic " + encodedAuth)
                    .addHeader("Accept", "application/json")
                    .build();

            Response response = client.newCall(request).execute();
            String message = response.body().string();

            if (response.code() == 204) {
                log.info("✅ Todas las órdenes canceladas exitosamente para la cuenta {}", accountId);
            } else {
                log.warn("⚠️ Cancelación múltiple fallida ({}): {}", response.code(), message);
            }

        } catch (IOException e) {
            log.error("❌ Error al cancelar todas las órdenes: {}", e.getMessage(), e);
        }
    }


    private void reconnectDelay() {
        try {
            Thread.sleep(5000); // Espera 5 segundos antes de reintentar
        } catch (InterruptedException ignored) {
        }
    }


    @Override
    public void startApplication() {
    }

    @Override
    public void stopApplication() {
    }


    @Override
    public void putNewOrderSingle(Message msg, RoutingMessage.Order orders) throws NullPointerException, SessionNotFound, FieldNotFound {

        try {

            OkHttpClient client = new OkHttpClient();

            String base = "https://broker-api.sandbox.alpaca.markets";

            String credentials = key + ":" + secret;
            String basicAuth = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());

            JSONObject json = new JSONObject();
            json.put("client_order_id", orders.getId());
            json.put("symbol", orders.getSymbol());
            json.put("qty", orders.getOrderQty());
            json.put("side", orders.getSide().name().toLowerCase());
            json.put("type", orders.getOrdType().name().toLowerCase());
            json.put("limit_price", String.valueOf(orders.getPrice()));
            json.put("time_in_force", "day");
            json.put("commission", orders.getCommission());
            json.put("commission_type", orders.getCommissionType());

            log.info("se envia orden a alpaca {}",  json.toString());


            Request request = new Request.Builder()
                    .url(base + "/v1/trading/accounts/" + orders.getAccount() + "/orders")
                    .addHeader("Authorization", basicAuth)
                    .addHeader("Content-Type", "application/json")
                    .post(RequestBody.create(json.toString(), MediaType.parse("application/json")))
                    .build();

            Response response = client.newCall(request).execute();
            String message = response.body().string();

            if (response.code() != 200) {
                RoutingMessage.Order.Builder order = orders.toBuilder();
                order.setExecId(IDGenerator.getID());
                order.setOrdStatus(RoutingMessage.OrderStatus.REJECTED);
                order.setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED);
                order.setText(message);
                sellsideManager.tell(order.build(), ActorRef.noSender());
                return;
            }

            RoutingMessage.Order.Builder order = orders.toBuilder();
            mapsOrders.put(orders.getId(), order);
            mapsOrders.put(msg.getString(ClOrdID.FIELD), order);

        } catch (Exception e) {

            log.error(e.getMessage(), e);
            RoutingMessage.Order ordersRejected = orders.toBuilder()
                    .setOrdStatus(RoutingMessage.OrderStatus.REJECTED)
                    .setText(e.getMessage())
                    .setTime(TimeGenerator.getTimeProto())
                    .setExecId(IDGenerator.getID())
                    .setExecType(RoutingMessage.ExecutionType.EXEC_REJECTED).build();
            sellsideManager.tell(ordersRejected, ActorRef.noSender());

        }

    }

    @Override
    public void putOrderReplace(Message msg, RoutingMessage.Order orders) throws NullPointerException, SessionNotFound, FieldNotFound {

        try {

            OkHttpClient client = new OkHttpClient();

            String credentials = key + ":" + secret;
            String basicAuth = "Basic " + Base64.getEncoder().encodeToString(credentials.getBytes());

            String cl = msg.getString(ClOrdID.FIELD);

            JSONObject json = new JSONObject();
            json.put("qty", msg.getString(38));
            json.put("time_in_force", "day");
            json.put("limit_price", msg.getString(44));
            json.put("trail", msg.getString(44));
            //json.put("client_order_id", cl);
            json.put("client_order_id", orders.getOrderID());
            json.put("commission", orders.getCommission());
            json.put("commission_type", orders.getCommissionType());

            log.info("remplazo enviado {}", json);

            String base = "https://broker-api.sandbox.alpaca.markets";

            Request request = new Request.Builder()
                    .url(base + "/v1/trading/accounts/" + orders.getAccount() + "/orders/" + orders.getOrderID())
                    .patch(RequestBody.create(json.toString(), MediaType.parse("application/json")))
                    .addHeader("Authorization", basicAuth)
                    .addHeader("Content-Type", "application/json")
                    .build();

            Response response = client.newCall(request).execute();
            String message = response.body().string();

            if (response.code() != 200) {
                log.error("rejected {} {} {}", response.code(), message, response);
                RoutingMessage.OrderCancelReject.Builder order = RoutingMessage.OrderCancelReject.newBuilder();
                order.setId(orders.getId());
                order.setText(message);
                sellsideManager.tell(order.build(), ActorRef.noSender());
                return;
            }

            RoutingMessage.Order.Builder order = orders.toBuilder();
            mapsOrders.put(orders.getId(), order);
            mapsOrders.put(orders.getOrderID(), order);
            mapsOrders.put(cl, order);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }

    @Override
    public void putOrderCancel(Message msg, RoutingMessage.Order orders) throws NullPointerException, SessionNotFound, FieldNotFound {

        try {

            RoutingMessage.Order.Builder order = mapsOrders.get(orders.getId());

            OkHttpClient client = new OkHttpClient();

            String credentials = key + ":" + secret;
            String encodedAuth = Base64.getEncoder().encodeToString(credentials.getBytes());

            String idCancel = order.getOrderID();

            log.info("cancel enviado order ID {}", idCancel);


            String url = String.format(
                    "https://broker-api.sandbox.alpaca.markets/v1/trading/accounts/%s/orders/%s",
                    order.getAccount(), idCancel
            );

            Request request = new Request.Builder()
                    .url(url)
                    .delete()
                    .addHeader("Authorization", "Basic " + encodedAuth)
                    .addHeader("Accept", "application/json")
                    .build();

            Response response = client.newCall(request).execute();
            String message = response.body().string();

            if (response.code() == 422 || response.code() == 404) {
                RoutingMessage.OrderCancelReject.Builder ordersr = RoutingMessage.OrderCancelReject.newBuilder();
                ordersr.setId(order.getId());
                ordersr.setText(message);
                sellsideManager.tell(ordersr.build(), ActorRef.noSender());
                return;
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

    }


}

