#include "kafka_rest_api.h"
#include <QNetworkReply>
#include <QtCore>
#include <QJsonDocument>
#include <qcontainerfwd.h>
#include <qhttpheaders.h>
#include <qjsondocument.h>
#include <qjsonobject.h>
#include <qnetworkrequest.h>
#include <qprotobufserializer.h>
#include <qstringview.h>

KafkaRestApi::KafkaRestApi(const QString& group) : mGroup{group} {
    QSettings settings;
    mServer = settings.value("server").toString();
    qDebug() << "mServer = " << mServer;
    mReadTimeout = settings.value("readTimeout").toInt();
    mManager.setAutoDeleteReplies(false);
}



void KafkaRestApi::createInstance() {
    auto request = QNetworkRequest(consumersUrl());
    request.setHeader(QNetworkRequest::ContentTypeHeader, "application/vnd.kafka.v2+json");

    mInstance = mGroup;
    QJsonObject parameters = {
        {"name", mInstance },
        {"auto.offset.reset", "earliest"}, //on the first time when we know nothing of the group, which is the offset we want to go
        {"format", "binary"},
        {"auto.commit.enable", true},
        {"fetch.min.bytes", 1}, //when at least 1 byte is available - send it immediately
        {"consumer.request.timeout.ms", mReadTimeout}
    };

    QJsonDocument doc(parameters);
    auto jsonPayload = doc.toJson(QJsonDocument::Compact);
    auto reply = mManager.post(request, jsonPayload);
    auto processResponse = [this, reply]{
        reply->deleteLater();
        QJsonObject obj;
        if (reply->error() != QNetworkReply::NoError) {
            mLastError = reply->errorString();
            mLastError += reply->readAll();
            emit error();
            return;
        }

        auto doc = QJsonDocument::fromJson(reply->readAll());
        auto root = doc.object();
        mInstance = root["instance_id"].toString();
        if (mInstance.isEmpty()) {
            mLastError = "failed to read instanceId";
            emit error();
        } else {
            emit instanceCreated();
        }
    };
    connect(reply, &QNetworkReply::finished, processResponse);
}


void KafkaRestApi::deleteInstance() {
    auto request = QNetworkRequest(consumersUrl(""));
    request.setHeader(QNetworkRequest::ContentTypeHeader, "application/vnd.kafka.v2+json");

    auto reply = mManager.deleteResource(request);
    auto processResponse = [this, reply]{
        reply->deleteLater();

        QJsonObject obj;
        if (reply->error() != QNetworkReply::NoError) {
            mLastError = reply->errorString();
            emit error();
        }
        qDebug().noquote() << "delete request for instance" << mInstance  << "group" << mGroup << "completed with" << reply->error();
        emit instanceDeleted();
    };
    connect(reply, &QNetworkReply::finished, processResponse);
}


void KafkaRestApi::subscribe(const QStringList& topics) {
    auto request = QNetworkRequest(consumersUrl("subscription"));
    request.setHeader(QNetworkRequest::ContentTypeHeader, "application/vnd.kafka.v2+json");

    QJsonArray topicsArray;
    for(const auto& topic: topics) {
        topicsArray.append(topic);
    }
    QJsonObject parameters = {
        {"topics", topicsArray}
    };
    
    QJsonDocument doc(parameters);
    auto jsonPayload = doc.toJson(QJsonDocument::Compact);
    auto reply = mManager.post(request, jsonPayload);
    auto processResponse = [this, reply]{
        reply->deleteLater();
        if (reply->error() == QNetworkReply::NoError) {
            emit subscribed();
        } else {
            mLastError = reply->errorString();
            emit error();
        }
    };
    connect(reply, &QNetworkReply::finished, processResponse);
}


void KafkaRestApi::consume() {
    auto request = QNetworkRequest(consumersUrl("records"));
    QHttpHeaders headers;
    headers.append("Content-Type", "application/vnd.kafka.v2+json");
    headers.append("Accept", "application/vnd.kafka.binary.v2+json");
    request.setHeaders(headers);

    auto reply = mManager.get(request);
    auto processResponse = [this, reply]{
        if (reply->error() != QNetworkReply::NoError) {
            mLastError = reply->errorString();
            qDebug() << "message consumption error: " << mLastError;
            emit error();
            return;
        }

        auto doc = QJsonDocument::fromJson(reply->readAll());
        for (const auto& item: doc.array()) {
            auto obj = item.toObject();
            auto value = obj["value"].toString().toUtf8();
            auto decoded = QByteArray::fromBase64(value);
            auto result = KafkaMessage{obj["topic"].toString(), obj["key"].toString(), decoded, 0/*timestamp?*/, obj["offset"].toInt()};
            emit message(result);
        }
        emit readingComplete();
        qDebug() << "reading complete";
    };
    connect(reply, &QNetworkReply::finished, processResponse);
}



void KafkaRestApi::produce(const QList<KafkaMessage>& messages) {
    if (messages.isEmpty()) {
        qWarning() << "no messages for sending";
        return;
    }
    
    auto request = QNetworkRequest(producerUrl(messages.first().topic));
    QHttpHeaders headers;
    headers.append("Content-Type", "application/vnd.kafka.binary.v2+json");
    request.setHeaders(headers);

    QJsonArray records;
    for (const auto& message: messages) {
        records.append(QJsonObject{
                {"key", message.key},
                {"value", QString{message.data.toBase64()}}
            });
    }
    QJsonObject parameters = {
        {"records", records},
    };
    QJsonDocument doc(parameters);
    auto reply = mManager.post(request, doc.toJson(QJsonDocument::Compact));
    auto processResponse = [this, reply]{
        if (reply->error() != QNetworkReply::NoError) {
            mLastError = reply->errorString();
            emit error();
            qDebug() << "message production error: " << mLastError;
            return;
        }

        auto doc = QJsonDocument::fromJson(reply->readAll());
    };
    connect(reply, &QNetworkReply::finished, processResponse);
}
