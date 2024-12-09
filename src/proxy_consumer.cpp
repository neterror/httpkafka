#include <QtCore>
#include <qcoreapplication.h>
#include <qfinalstate.h>
#include "proxy_consumer.h"
#include "kafka_rest_api.h"

#define NewWrapperNode(x,p) auto x = new QState(p); x->setObjectName(#x)
#define NewNode(x,p,f) auto x = new QState(p); x->setObjectName(#x); connect(x, &QState::entered, this, f)

ProxyConsumer::ProxyConsumer(const QString &groupName,
                             const QStringList &topics) : mProxy(groupName), mTopics(topics)
{
    NewWrapperNode(work, &mSM);

    NewNode(cleanup,             &mSM, &ProxyConsumer::onCleanup);
    NewNode(epilogue,            &mSM, &ProxyConsumer::onEpilogue);

    NewNode(createInstance,      work, &ProxyConsumer::onCreateInstance);
    NewNode(deleteInstance,      work, &ProxyConsumer::onDeleteInstance);
    NewNode(subscribe,           work, &ProxyConsumer::onSubscribe);
    NewNode(consume,             work, &ProxyConsumer::onConsumeMessages);
    NewNode(generalErrorHandler, work, &ProxyConsumer::onGeneralErrorHandler);

    work->addTransition(&mProxy, &KafkaRestApi::error, generalErrorHandler); //on any error, start again from the create instance
    work->addTransition(this, &ProxyConsumer::finish, cleanup);

    //1. try to create the instance
    createInstance->addTransition(&mProxy, &KafkaRestApi::instanceCreated, subscribe);

    //2. in case of instance creation error, we have the signal before the wrapping work node. Delete the existing instance
    createInstance->addTransition(&mProxy, &KafkaRestApi::error, deleteInstance); 
    deleteInstance->addTransition(&mProxy, &KafkaRestApi::instanceDeleted, createInstance);  //when deleted, create instance again
    
    //3. in case of successful instance creation, subscribe 
    subscribe->addTransition(&mProxy, &KafkaRestApi::subscribed, consume);

    //4. consume
    consume->addTransition(&mProxy, &KafkaRestApi::readingComplete, consume);

    //5. in case of error, try again to create the instance
    generalErrorHandler->addTransition(this, &ProxyConsumer::tryAgain, createInstance);

    //6. stop command. Delete the instance and exit
    cleanup->addTransition(&mProxy, &KafkaRestApi::instanceDeleted, epilogue);
    cleanup->addTransition(&mProxy, &KafkaRestApi::error, epilogue);

    work->setInitialState(createInstance);
    mSM.setInitialState(work);

    connect(&mProxy, &KafkaRestApi::message, this, &ProxyConsumer::message);
}


void ProxyConsumer::start() {
    mSM.start();
}

// this method may be called from the global context of the signal handler
// make sure it is serialized with the event loop
void ProxyConsumer::stop() {
    QTimer::singleShot(0, [this]{emit finish();});
}


void ProxyConsumer::onCreateInstance() {
    qDebug() << "request instance";
    mProxy.createInstance();
}

void ProxyConsumer::onDeleteInstance() {
    qDebug() << "request to delete the current instance";
    mProxy.deleteInstance();
}


void ProxyConsumer::onSubscribe() {
    qDebug() << "instance created. subscribe to" << mTopics;
    mProxy.subscribe(mTopics);
}

void ProxyConsumer::onConsumeMessages() {
    qDebug() << "start message consumption";
    mProxy.consume();
}

void ProxyConsumer::onGeneralErrorHandler() {
    qDebug() << "error: " << mProxy.lastError();
    QTimer::singleShot(1000, [this]{emit tryAgain();});
}

void ProxyConsumer::onCleanup() {
    qDebug() << "cleanup requested. Delete the instance";
    mProxy.deleteInstance();
}

void ProxyConsumer::onEpilogue() {
    qDebug() << "onEpilogue reached, intance deleted";
    QCoreApplication::quit();
}
