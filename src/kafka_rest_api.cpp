#include "kafka_rest_api.h"
#include <QAuthenticator>
#include <QNetworkReply>
#include <QtCore>
#include <QJsonDocument>
#include <qcontainerfwd.h>
#include <qhttpheaders.h>
#include <qjsondocument.h>
#include <qjsonobject.h>
#include <qnetworkrequest.h>
#include <qstringview.h>

KafkaRestApi::KafkaRestApi(const QString& group) : mGroup{group} {
    QSettings settings;
    mServer = settings.value("KafkaRestApi/server").toString();
    mReadTimeout = settings.value("KafkaRestApi/readTimeout").toInt();
    qDebug() << "mServer = " << mServer << ", readTimeout: " << mReadTimeout;
    mManager.setAutoDeleteReplies(false);

    auto user = settings.value("KafkaRestApi/user").toString();
    auto pass = settings.value("KafkaRestApi/password").toString();
    QObject::connect(&mManager, &QNetworkAccessManager::authenticationRequired,
                     [user, pass](QNetworkReply* reply, QAuthenticator* authenticator) {
                         qDebug() << "authenticating with " << user << pass;
                         authenticator->setUser(user);
                         authenticator->setPassword(pass);
                     });
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
        auto instance = root["instance_id"].toString();
        if (instance.isEmpty()) {
            mLastError = "failed to read instanceId";
            emit error();
        } else {
            mInstance = instance;
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
    request.setHeader(QNetworkRequest::ContentTypeHeader, "application/vnd.kafka.v2+json");

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
            processMessage(item.toObject());
        }
        emit readingComplete();
    };
    connect(reply, &QNetworkReply::finished, processResponse);
}


void KafkaRestApi::processMessage(const QJsonObject& obj) {
    auto value = obj["value"].toString().toUtf8();
    auto decoded = QByteArray::fromBase64(value);
    auto topic = obj["topic"].toString();
    auto key = obj["key"].toString();
    auto offset = obj["offset"].toInt();


    emit message({topic, key, decoded, 0, offset});
}


void KafkaRestApi::produce(const QList<KafkaMessage>& messages) {
    if (messages.isEmpty()) {
        qWarning() << "no messages for sending";
        return;
    }
    
    auto request = QNetworkRequest(producerUrl(messages.first().topic));
    request.setHeader(QNetworkRequest::ContentTypeHeader, "application/vnd.kafka.binary.v2+json");
    request.hasRawHeader("Accept: application/vnd.kafka.v2+json");

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
