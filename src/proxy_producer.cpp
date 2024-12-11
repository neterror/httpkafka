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


void ProxyProducer::send(KafkaMessage message, bool delayedSend) {
    auto moved = std::move(message);
    QDateTime UTC(QDateTime::currentDateTimeUtc());
    moved.timestamp = UTC.toMSecsSinceEpoch();

    if (delayedSend && mSendDelay.isActive()) {
        mMessages[moved.topic].append(moved);
    } else {
        //send directly
        mProxy.produce({moved});
    }
}


void ProxyProducer::onTimeout() {
    for (const auto& key: mMessages.keys()) {
        mProxy.produce(mMessages[key]);
    }
    mMessages.clear();
}

void ProxyProducer::stop() {
    mSendDelay.stop();
    mMessages.clear();
}
