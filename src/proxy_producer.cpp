#include "proxy_producer.h"
#include "kafka_rest_api.h"

//the group name doesn't matter for the producer - it is for the consumers only
ProxyProducer::ProxyProducer(int delay) : mProxy("producer") {
    if (delay) {
        mSendDelay.setSingleShot(false);
        mSendDelay.setInterval(delay);
        connect(&mSendDelay, &QTimer::timeout, this, &ProxyProducer::onTimeout);
        mSendDelay.start();
    }

    mTimer.start(); //measures the time 
}


void ProxyProducer::send(KafkaMessage message) {
    auto moved = std::move(message);
    QDateTime UTC(QDateTime::currentDateTimeUtc());
    moved.timestamp = UTC.toMSecsSinceEpoch();

    if (mSendDelay.isActive()) {
        mMessages.append(moved);
    } else {
        //send directly
        mProxy.produce({moved});
    }
}


void ProxyProducer::onTimeout() {
    mProxy.produce(mMessages);
    mMessages.clear();
}
