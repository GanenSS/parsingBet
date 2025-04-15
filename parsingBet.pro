QT = core
QT += sql

CONFIG += c++17 cmdline

# You can make your code fail to compile if it uses deprecated APIs.
# In order to do so, uncomment the following line.
#DEFINES += QT_DISABLE_DEPRECATED_BEFORE=0x060000    # disables all the APIs deprecated before Qt 6.0.0

SOURCES += \
        main.cpp \
        passBD/passdb.cpp

# Default rules for deployment.
qnx: target.path = /tmp/$${TARGET}/bin
else: unix:!android: target.path = /opt/$${TARGET}/bin
!isEmpty(target.path): INSTALLS += target

DISTFILES += \
    data/am-football.json \
    data/australian-football.json \
    data/baseball.json \
    data/basketball.json \
    data/billiard.json \
    data/boxing.json \
    data/cricket.json \
    data/darts.json \
    data/esports.json \
    data/floorball.json \
    data/football.json \
    data/futsal.json \
    data/gaelic-football.json \
    data/handball.json \
    data/hockey.json \
    data/lacrosse.json \
    data/mix-fights.json \
    data/rugby.json \
    data/softball.json \
    data/sport-2.json \
    data/squash.json \
    data/table-tennis.json \
    data/tennis.json \
    data/volleyball.json \
    data/waterpolo.json \
    parsak.py

HEADERS += \
    passBD/passdb.h
