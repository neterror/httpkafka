#pragma once

#include <qjsonobject.h>
#include <QJsonObject>
#include <QNetworkAccessManager>
#include "kafka_message.h"


class KafkaRestApi : public QObject {
    Q_OBJECT

    QNetworkAccessManager mManager;
    QString mServer;
    QString mInstance;
    QString mGroup;
    qint32 mReadTimeout;
    QString mLastError;

    QUrl consumersUrl() const {
        return QUrl{QString("%1/consumers/%2").arg(mServer).arg(mGroup)};
    }

    QUrl consumersUrl(const QString& command) const {
        return QUrl{QString("%1/consumers/%2/instances/%3/%4")
                    .arg(mServer)
                    .arg(mGroup)
                    .arg(mInstance)
                    .arg(command)};
    }

    QUrl producerUrl(const QString& topic) const {
        return QUrl{QString("%1/topics/%2").arg(mServer).arg(topic)};
    }
    

public:
    KafkaRestApi(const QString& group);
    QString lastError() const {return mLastError;}

    void createInstance();
    void deleteInstance();
    void subscribe(const QStringList& topics);
    void consume();
    void produce(const QList<KafkaMessage>& messages);
    
signals:
    void instanceCreated();
    void instanceDeleted();
    void subscribed();
    void message(KafkaMessage message);
    void readingComplete();
    void error();
};
