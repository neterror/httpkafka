#pragma once
#include <QString>
#include <QByteArray>

struct KafkaMessage {
    QString topic;
    QString key;
    QByteArray data;
    qint64 timestamp;
    qint64 offset;
};
