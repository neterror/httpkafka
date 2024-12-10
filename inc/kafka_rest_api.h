#pragma once

#include <qjsonobject.h>
#include <QJsonObject>
#include <QNetworkAccessManager>
#include "kafka_message.h"
#include <QElapsedTimer>

class KafkaRestApi : public QObject {
    Q_OBJECT

    QNetworkAccessManager mManager;
    QString mServer;
    QString mInstance;
    QString mGroup;
    qint32 mReadTimeout;
    QString mLastError;
    QElapsedTimer mElapsed;
    bool mReplayWithTimeDelay {false};
    

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
    
    void processMessage(const QJsonObject& obj);

public:
    KafkaRestApi(const QString& group);
    QString lastError() const {return mLastError;}

    void replayWithTimeDelays(bool enable) {mReplayWithTimeDelay = enable;}

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
