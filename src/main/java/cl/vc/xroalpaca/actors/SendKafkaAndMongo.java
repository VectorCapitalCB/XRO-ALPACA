package cl.vc.xroalpaca.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import cl.vc.xroalpaca.MainApp;
import cl.vc.xroalpaca.util.KafkaAdapter;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.util.JsonFormat;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.bson.Document;
import org.json.JSONObject;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;


@Slf4j
public class SendKafkaAndMongo extends AbstractActor {

    private JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();

    private KafkaAdapter kafkaAdapter;

    private MongoClient mongoClient;

    private MongoDatabase database;

    private Boolean isConnected;

    private MongoCollection<Document> collection;

    private SendKafkaAndMongo(Properties properties) {
        try {

            if (Boolean.valueOf(properties.getProperty("app.stream.kafka.binder.offset.start"))) {
                kafkaAdapter = new KafkaAdapter(properties);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static Props props(Properties properties) {
        return Props.create(SendKafkaAndMongo.class, properties);
    }

    @Override
    public void preStart() {
        try {

            String url = MainApp.getProperties().getProperty("mongo.connection");
            String collectionName = MainApp.getProperties().getProperty("security-exchange");
            String db = MainApp.getProperties().getProperty("mongo.db");

            isConnected = Boolean.valueOf(MainApp.getProperties().getProperty("mongo.isconnected"));

            if (isConnected) {
                MongoClientURI uri = new MongoClientURI(url);
                mongoClient = new MongoClient(uri);
                database = mongoClient.getDatabase(db);
                collection = database.getCollection(collectionName);
                log.info("se crea conexión kafka");
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RoutingMessage.OrderCancelReject.class, this::onOrderCancelReject)
                .match(RoutingMessage.Order.class, this::onOrder)
                .match(NotificationMessage.Notification.class, this::onNotification)
                .build();
    }


    private void onNotification(NotificationMessage.Notification msg) {
        try {

            String topic = "trade-monitor-notification-" + msg.getSecurityExchange();
            String message = printer.print(msg);

            if (kafkaAdapter != null) {
                kafkaAdapter.getProducer().send(new ProducerRecord<>(topic, message));
            }


        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }

    private void onOrderCancelReject(RoutingMessage.OrderCancelReject msg) {
        try {

            String topic = "trade-monitor-rejected";
            String message = printer.print(msg);

            if (kafkaAdapter != null) {
                kafkaAdapter.getProducer().send(new ProducerRecord<>(topic, message));
            }


        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }

    private void onOrder(RoutingMessage.Order msg) {
        try {

            String topic = "trade-monitor-" + msg.getSecurityExchange().name();
            String message = printer.print(msg);

            if (kafkaAdapter != null) {
                kafkaAdapter.getProducer().send(new ProducerRecord<>(topic, message));
            }

            if (msg.getExecType().equals(RoutingMessage.ExecutionType.EXEC_TRADE) && collection != null) {

                Instant instant = Instant.ofEpochSecond(msg.getTime().getSeconds(), msg.getTime().getNanos());
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
                String humanReadableDate = formatter.format(instant);

                JSONObject jsonpa = new JSONObject(message);
                Document document = new Document("execId", msg.getExecId())
                        .append("protobufData", jsonpa.toString())
                        .append("time", humanReadableDate)
                        .append("symbol", msg.getSymbol())
                        .append("side", msg.getSide().name())
                        .append("id", msg.getId())
                        .append("prefix", msg.getPrefixID())
                        .append("securityExchange", msg.getSecurityExchange().name());

                collection.insertOne(document);
            }

            //log.info("send kafka and mongo {}", message);

        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }

}
