#include <QtCore>
#include "kafka_message.h"
#include "proxy_consumer.h"
#include "proxy_producer.h"
#include "position_report.qpb.h"

#include <memory>
#include <qcommandlineparser.h>
#include <qcoreapplication.h>
#include <qprotobufserializer.h>
#include <signal.h>
#include <unistd.h>

static std::unique_ptr<ProxyConsumer> _consumer;
static std::unique_ptr<ProxyProducer> _producer;

static void cleanExit(int) {
    if (_consumer) {
        _consumer->stop();
    }
}


void sendMessage() {
    QProtobufSerializer serializer;
    PositionReport report;
    report.setLat(42.641713);
    report.setLng(23.3723);
    report.setAlt(0);
    report.setSpeed(0);
    auto bytes = report.serialize(&serializer);
    _producer->send({"abrites.position", "866760051945394", bytes});
}


std::unique_ptr<QTimer> startProducer() {
    auto timer = std::make_unique<QTimer>();
    timer->setSingleShot(false);
    timer->setInterval(1000);
    QObject::connect(timer.get(), &QTimer::timeout, sendMessage);
    _producer.reset(new ProxyProducer(0));
    timer->start();
    return timer;
}

void receiveMessage(KafkaMessage message) {
    QProtobufSerializer serializer;
    PositionReport report;
    if (!report.deserialize(&serializer, message.data)) {
        qWarning() << "Failed to deserialize PositionReport message";
    } else {
        qDebug().noquote() << "lat: " << report.lat() << "lng:" << report.lng() << ", key: " << message.key;
    }
}
    


int main(int argc, char** argv) {
    QCoreApplication app(argc, argv);

    app.setApplicationName("kafka_rest_api");
    app.setOrganizationName("abrites");


    std::unique_ptr<QTimer> timer;
    QCommandLineParser parser;
    parser.addOptions({
            {"consume", "start consumer"},
            {"produce", "start producer"}
        });
    parser.process(app);
    if (parser.isSet("consume")) {
        signal(SIGINT, cleanExit);
        signal(SIGTERM, cleanExit);

        _consumer.reset(new ProxyConsumer("ardos_866760051945394", {"abrites.position"}));
        QObject::connect(_consumer.get(), &ProxyConsumer::message, receiveMessage);
        _consumer->start();
    } else {
        if (parser.isSet("produce")) {
            timer = startProducer();
        } else {
            parser.showHelp();
        }
    }

    auto exitCode = app.exec();
    qDebug() << "The program finishes execution with exit code" << exitCode;
    timer.reset(nullptr);
    return exitCode;
}
