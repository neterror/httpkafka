#pragma once
#include <QTimer>
#include <QElapsedTimer>
#include <QQueue>
#include <QObject>
#include <QtStateMachine/qstatemachine.h>

#include "kafka_message.h"
#include "kafka_rest_api.h"

class ProxyProducer : public QObject {
    Q_OBJECT
    KafkaRestApi mProxy;
    QStateMachine mSM;
                        
    QQueue<KafkaMessage> mQueue;
private slots:
    void onSend();
    void onWaitForData();
public:
    //if messageDelay == 0, send the messages immediately. Otherwise, accumulate the messages for the specified interval before sending
    explicit ProxyProducer(int messageDelay);

    //we append the timestamp at the time when we accepted the message for sending
    //delayedSend accumulates the messages for mSendDelay time before sending at once
    
    void stop();
    void send(KafkaMessage messages);
signals:
    void newData();
    void error();
};

