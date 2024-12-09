#pragma once
#include <QTimer>
#include <QElapsedTimer>
#include <QObject>
#include <QtStateMachine/qstatemachine.h>

#include "kafka_message.h"
#include "kafka_rest_api.h"

class ProxyProducer : public QObject {
    Q_OBJECT
    KafkaRestApi mProxy;
    QList<KafkaMessage> mMessages;
    QTimer mSendDelay;
    QElapsedTimer mTimer;
private slots:
    void onTimeout();

public:
    //if messageDelay == 0, send the messages immediately. Otherwise, accumulate the messages for the specified interval before sending
    explicit ProxyProducer(int messageDelay);

    //we append the timestamp at the time when we accepted the message for sending
    void send(KafkaMessage messages);
};

