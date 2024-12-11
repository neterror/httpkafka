#include "proxy_producer.h"
#include "kafka_rest_api.h"
#include <qnamespace.h>

//the group name doesn't matter for the producer - it is for the consumers only
ProxyProducer::ProxyProducer(int delay) : mProxy("producer") {
    auto waitForData = new QState(&mSM);
    auto send = new QState(&mSM);

    waitForData->addTransition(this, &ProxyProducer::newData, send);
    send->addTransition(&mProxy, &KafkaRestApi::messageDelivered, waitForData);
    send->addTransition(&mProxy, &KafkaRestApi::error, waitForData);

    connect(waitForData, &QState::entered, this, &ProxyProducer::onWaitForData);
    connect(send, &QState::entered, this, &ProxyProducer::onSend);

    mSM.setInitialState(waitForData);
    mSM.start();
}

void ProxyProducer::onWaitForData() {
    if (!mQueue.isEmpty()) {
        emit newData();
    }
}


void ProxyProducer::onSend() {
    if (mQueue.isEmpty()) {
        qWarning() << "Empty queue for proxy send!";
        emit error();
        return;
    }
    auto data = mQueue.dequeue();
    qDebug() << "HTTP proxy is executing send";
    mProxy.produce({data});
}

void ProxyProducer::send(KafkaMessage data) {
    auto message = std::move(data);
    QDateTime UTC(QDateTime::currentDateTimeUtc());
    message.timestamp = UTC.toMSecsSinceEpoch();
    mQueue.enqueue(message);

    emit newData();
}

void ProxyProducer::stop() {
    mSM.stop();
}
