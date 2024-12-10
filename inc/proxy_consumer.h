#pragma once
#include "kafka_rest_api.h"
#include <QObject>
#include <QtStateMachine/qstatemachine.h>
#include <qtmetamacros.h>

class ProxyConsumer : public QObject {
    Q_OBJECT
    KafkaRestApi mProxy;

    QStringList mTopics;
    QStateMachine mSM;
public:
    ProxyConsumer(const QString& groupName, const QStringList& topics);
    void start();
    void stop();
private slots:
    void onCreateInstance();
    void onDeleteInstance();
    void onSubscribe();
    void onConsumeMessages();
    void onGeneralErrorHandler();
    void onCleanup();
    void onEpilogue();

signals:
    void message(KafkaMessage message);
    void error(QString message);
    void requestFinish();
    void finished();
    void tryAgain();
};

