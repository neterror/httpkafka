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
    QMap<QString, QList<KafkaMessage>> mMessages;
    QTimer mSendDelay;
    QElapsedTimer mTimer;
private slots:
    void onTimeout();
private slots:
    void onSend(KafkaMessage messages, bool delayedSend = true);
public:
    //if messageDelay == 0, send the messages immediately. Otherwise, accumulate the messages for the specified interval before sending
    explicit ProxyProducer(int messageDelay);

    //we append the timestamp at the time when we accepted the message for sending
    //delayedSend accumulates the messages for mSendDelay time before sending at once
    
    void stop();
    void send(KafkaMessage messages, bool delayedSend = true);

signals:
    void enqueue(KafkaMessage messages, bool delayedSend);
};

